"""Provides the archive task class."""

import glob
import os
import time
import boto3
import botocore.exceptions
from .common import settings
from .common import task
from .common import log_templates_pb2
from . import tools



class Boto3Transferer():

    def __init__(self, bucket, local_prefix, global_prefix, client_settings):
        self._bucket = bucket
        self._local_prefix = local_prefix
        self._global_prefix = global_prefix
        self._client_settings = client_settings


        self._session = boto3.session.Session()
        self._client = self._session.client(**self._client_settings)
        try:
            self._client.get_bucket_location(Bucket = self._bucket)
        except botocore.exceptions.ClientError:
            raise Exception(
                'Could not connect to remote storage. '
                'Are your settings correct?'
                )

    def _full_key(self, partial_key):
        return os.path.join(
            self._global_prefix,
            self._local_prefix,
            partial_key
            )

    def upload(self, file_path, object_key):
        md5 = tools.filesys.md5_hash(file_path)
        self._client.upload_file(
            Filename = file_path,
            Bucket = self._bucket,
            Key = self._full_key(object_key),
            ExtraArgs = {"Metadata": {"md5chksum": md5}}
            )
        return self._full_key(object_key)

    def download(self, object_key, file_path):
        self._client.download_file(
            Bucket = self._bucket,
            Key = self._full_key(object_key),
            Filename = file_path
            )

    def md5_hash(self, object_key):
        try:
            response = self._client.head_object(
                Bucket = self._bucket,
                Key = self._full_key(object_key)
                )
            return response['ResponseMetadata'][
                                'HTTPHeaders']['x-amz-meta-md5chksum']
        except botocore.exceptions.ClientError:
            print('Error?')
            raise self.ObjectDoesNotExist()


    class ObjectDoesNotExist(Exception):
        pass



class ArchiveTask(task.Task):
    """This class provides the mechanism for performing archive tasks.

    Archive tasks transfer compressed files (outputted from the compress
    task) from the local server to a bucket storage space. The rationale
    for this task is that even with compression the feeds may take up a
    large amount of space, and bucket storage is an order of magnitude
    cheaper than server storage. (At time of writing, Jan 5 2018, Digital
    Ocean's entry level server is $5/month and has 20GB of space; its
    entry level bucket storage option is also $5/month and has 250GB of
    space.)

    To use the archive task, initialize in the common way for all tasks:

        task = ArchiveTask(root_dir=, feeds=, quiet=, log_file_path=)

    see the task class for details on the arguments here. After this
    initialization it is necessary to set the bucket storage settings.
    These settings will be used by the Python boto3 package to interact
    with the storage. There are two attributes that need to be set.
    The first is a dictionary which will be passed directly to the
    boto3.session.Session().client() in order to establish the connection.
    The exact form of this will depend on the bucket storage provided;
    the example below is for Digital Ocean spaces. The second attribute
    is the name of the bucket.

        task.boto3_settings = {
            'service_name' : 's3',
            'region_name' : 'nyc3',
            'endpoint_url' : 'https://nyc3.digitaloceanspaces.com',
            'aws_access_key_id' : '[Get from Digital Ocean control panel]',
            'aws_secret_access_key' : '[Get from Digital Ocean control panel]'
            }
        task.bucket = 'my_bucket'

    Additional initialization may be desired by setting the limit attribute:

        task.limit = 10

    The task is then run using the run() method:

        task.run()

    Attributes:
        limit (int): an integer imposing an upper bound on the number of
                     compressed files to transfer to storage. If set to -1,
                     no limit is imposed. Default is -1.
        n_uploaded (int): the number of compressed files uploaded successfully.
        n_failed (int): the number of compressed files that failed to upload.
    """

    def __init__(self, **kwargs):

        super().__init__(**kwargs)
        # Initialize the log directory
        self._init_log('archive', log_templates_pb2.ArchiveTaskLog)

        # Initialize task configuration
        self.limit = -1
        self.file_access_lag = 120
        self._transferer = None

    def init_boto3_remote_storage(self, 
        bucket, local_prefix, global_prefix, client_settings):
        self._transferer = Boto3Transferer(
            bucket = bucket,
            local_prefix = local_prefix,
            global_prefix = global_prefix,
            client_settings = client_settings
            )

        

        
    def _run(self):
        """Internal method that performs that actual task run.
        
        Do NOT use this method directly!! Use run() instead.
        The run() method automatically handles logging and task-level 
        exception handling."""
        self._log_run_configuration()
        self._log.limit = self.limit
        self._log.file_access_lag = self.file_access_lag

        # Initialize variables and start the boto3 session
        self.num_uploaded = 0
        self.num_failed = 0
        #session = boto3.session.Session()
        #client = session.client(**self.boto3_settings)

        # Iterate over all archives in the compressed directory
        compressed_dir = os.path.join(
            self._storage_dir,
            'feeds',
            'compressed'
            )
        files = glob.iglob(os.path.join(compressed_dir, '*/*/*.tar.bz2'))

        for source_file_path in files:
            upload_log = self._log.UploadLog()
            # Check the last modification time; if it was within
            # file_access_lag seconds ignore this file to avoid file I/O
            # clash with a download task
            if(time.time()-os.path.getmtime(source_file_path)
               < self.file_access_lag):
                continue

            # The object key is just the relative path to the file
            # from the compressed feeds directory.
            target_key = source_file_path[len(compressed_dir)+1:]
            file_name = os.path.basename(target_key)
            feed_id = file_name[:file_name.find('-')]

            # Now move to upload.
            # It may be possible that a file already exists in storage with
            # the same name; in this case that archive and the local archive
            # need to be merged.
            # First calculate the md5 hash of the local file, we'll need this
            # anyway. Then see if the target_path is already in storage. If it
            # is, it may be just a duplicate (for instance, if two aggregating
            # instances are running on separate machines. The md5 hash will
            # tell us that. If they're different, we need to download the
            # remote version and merge it with the local version.
            md5 = tools.filesys.md5_hash(source_file_path)
            upload_log.md5_hash = md5
            upload_log.source_file_path = source_file_path
            upload_log.target_key = target_key
            upload_log.feed_id = feed_id
            need_to_upload = True
            #self.log.write('Preparing to upload ' + file_name)
            #self.log.write('    target: ' + target_path)
            #self.log.write('    md5: ' + md5)
            try:
                existing_md5 = self._transferer.md5_hash(target_key)
                upload_log.preexisting_md5_hash = existing_md5
                #self.log.write(`
                #        '    The target path exists already; '
                #        'its md5 hash is ' + existing_md5)
            except self._transferer.ObjectDoesNotExist:
                existing_md5 = None
                upload_log.preexisting_md5_hash = ''

            if md5 == existing_md5:
                need_to_upload = False
            elif existing_md5 is not None:



                # One can't directly merge bz2 files. We create a
                # temporary directory, extract both archives into it,
                # and then make one archive from the files inside.
                self._print('Merging local file with preexisting file.')
                temp_dir = os.path.join(self._storage_dir, 'tmp_merge')
                temp_file_path = os.path.join(temp_dir, 'existing.tar.bz2')
                tools.filesys.ensure_dir(temp_dir)
                try:
                    self._transferer.download(target_key, temp_file_path)
                except Exception as e:
                    self._print(
                        'Failed to download prexisting file to merge. '
                        'Skipping upload.'
                        )
                    self._print('Reason: {}.'.format(e))
                    self.num_failed += 1
                    upload_log.success = False
                    task.add_exception_to_log(e, upload_log.download_error)
                    self._log.uploads.extend([upload_log])
                    continue

                tools.filesys.tar_file_to_directory(temp_file_path, temp_dir)
                tools.filesys.tar_file_to_directory(source_file_path, temp_dir)
                tools.filesys.directory_to_tar_file(temp_dir, source_file_path)


            if need_to_upload:
                try:
                    key = self._transferer.upload(source_file_path, target_key)
                except Exception as e:
                    self._print('Failed to upload file.')
                    self._print('Reason: {}.'.format(e))

                    self.num_failed += 1
                    upload_log.success = False
                    task.add_exception_to_log(e, upload_log.upload_error)
                    self._log.uploads.extend([upload_log])
                    continue

            upload_log.success = True
            self._log.uploads.extend([upload_log])
            self.num_uploaded += 1
            os.remove(source_file_path)
            self._print('Uploaded {}\n  -> {}.'.format(source_file_path, key))

            # Check if the processing limit has been reached
            cond1 = self.limit >= 0
            cond2 = self.num_uploaded + self.num_failed >= self.limit
            if cond1 and cond2:
                self._print('Reached file processing limit.')
                self._print('Run task again to archive more files.')
                break

        total = tools.filesys.prune_directory_tree(compressed_dir)

        self._print('Archive task ended. Statistics:')
        self._print('  * {} files uploaded.'.format(self.num_uploaded))
        self._print('  * {} files failed to upload.'.format(self.num_failed))



