from StringIO import StringIO

from django.core.files.storage import FileSystemStorage
from django.core.files.base import File

from boto.exception import S3ResponseError, S3CreateError, BotoClientError

from django.core.cache import cache
from django.conf import settings

AWS_ACCESS_KEY = getattr(settings, 'AWS_ACCESS_KEY')
AWS_SECRET_KEY = getattr(settings, 'AWS_SECRET_KEY')
S3_STORAGE_BUCKET = getattr(settings, 'S3_STORAGE_BUCKET')
S3_CNAME = getattr(settings, 'S3_CNAME', False)

BACKUP_MEDIA_URL = getattr(settings, 'BACKUP_MEDIA_URL')
MEDIA_URL = getattr(settings, 'MEDIA_URL')

MEDIA_ROOT = getattr(settings, 'MEDIA_ROOT')

S3_ACL = getattr(settings, 'S3_ACL', 'public-read')
S3_HEADERS = getattr(settings, 'S3_HEADERS', {})

S3_FAR_FUTURE = getattr(settings, 'S3_FAR_FUTURE', True)
S3_HASHED_NAME = getattr(settings, 'S3_HASHED_NAME', True)

if S3_FAR_FUTURE:
    from datetime import timedelta, datetime
    future = datetime.now() + timedelta(3650) # ten years into the future seems like enough
    future = future.strftime('%a, %d %b %Y 20:00:00 GMT')
    S3_HEADERS['Expires'] = future

class SimpleStorage(FileSystemStorage):
    """
    S3 storage
    """

    def __init__(self, location=None, base_url=None):
        super(SimpleStorage, self).__init__()

        self.acl = S3_ACL
        self.headers = S3_HEADERS
        self.bucket = S3_STORAGE_BUCKET

    def _get_bucket(self):
        from boto.s3.connection import S3Connection
        connection = S3Connection(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        try:
            bucket = connection.get_bucket(self.bucket)
        except S3ResponseError:
            raise IOError("% bucket does not exist.  Create bucket before using storage backend." % bucket)
        return bucket

    def _open(self, name, mode='rb'):
        s3file = S3BotoStorageFile(name, mode, self)
        if s3file:
            return s3file
        else:
            return super(SimpleStorage, self)._open(name, mode)

    def _save(self, name, content):
        if S3_HASHED_NAME:
            import os, hashlib
            folder, file = name.split('/')
            name, ext = os.path.splitext(file)       
            name = hashlib.md5(content.read()).hexdigest()
            name = folder + '/' + name + ext
            try:
                super(SimpleStorage, self).delete(name)
            except:
                pass

        name = super(SimpleStorage, self)._save(name, content) 
        try:
            from simplestorage.tasks import S3PushTask
            task = S3PushTask()
            task.delay(name, MEDIA_ROOT + name, self.acl, self.bucket, AWS_ACCESS_KEY, AWS_SECRET_KEY)
        except:
            from simplestorage.utils import S3Push
            S3Push(name, MEDIA_ROOT + name, self.acl, self.bucket, AWS_ACCESS_KEY, AWS_SECRET_KEY)
        return name

    def delete(self, name):
        super(SimpleStorage, self).delete(name)
        bucket = self._get_bucket()
        bucket.delete_key(name)

    def exists_on_s3(self, name):
        try:
            bucket = self._get_bucket()
            s3 = bool(bucket.get_key(name))
        except: 
            s3 = False
        return s3

    def size(self, name):
        try:
            return super(SimpleStorage, self).size(name)
        except OSError:
            bucket = self._get_bucket()
            s3file = bucket.get_key(name)
            return s3file.size

    def url(self, name):
        url = cache.get(name)
        if url:
            return url
        else:
            bucket = self._get_bucket()
            s3file = bucket.get_key(name)
            if s3file:
                url = s3file.generate_url(0, query_auth=False, force_http=True)
                if S3_CNAME:
                    url = url.replace(self.bucket + '.s3.amazonaws.com', S3_CNAME)
                cache.set(name, url)
                return url
            else:
                return super(SimpleStorage, self).url(name).replace(MEDIA_URL, BACKUP_MEDIA_URL)

class S3BotoStorageFile(File):
    def __init__(self, name, mode, storage):
        self._storage = storage
        self.name = name
        self._mode = mode
        self.key = storage._get_bucket().get_key(name)
        self._is_dirty = False
        self.file = StringIO()

    @property
    def size(self):
        return self.key.size

    def read(self, *args, **kwargs):
        self.file = StringIO()
        self._is_dirty = False
        self.key.get_contents_to_file(self.file)
        return self.file.getvalue()

    def write(self, content):
        if 'w' not in self._mode:
            raise AttributeError("File was opened for read-only access.")
        self.file = StringIO(content)
        self._is_dirty = True

    def close(self):
        if self._is_dirty:
            self.key.set_contents_from_string(self.file.getvalue(), headers=self._storage.headers, acl=self._storage.acl)
        self.key.close()
