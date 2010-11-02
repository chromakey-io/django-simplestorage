from celery.task import Task

class S3PushTask(Task):
    def run(self, name, path, acl, bucket, key, secret):
        import mimetypes
        from boto.s3.connection import S3Connection

        connection = S3Connection(key, secret)
        try:
            bucket = connection.get_bucket(bucket)
        except S3ResponseError:
            raise IOError("% bucket does not exist.  Create bucket before using storage backend." % bucket)

        content_type = mimetypes.guess_type(name)[0] or "application/x-octet-stream"

        file = open(path)

        k = bucket.new_key(name)
        k.set_contents_from_file(file, policy=acl)
