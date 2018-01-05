"""Provides the archive action class."""

import glob
import os
import time
import boto3
import botocore.exceptions
from . import settings
from . import action
from . import tools

class ArchiveAction(action.Action):
    """This class provides the mechanism for performing archive actions.

    Archive actions transfer compressed files (outputted from the compress action) from the local server to a bucket storage
    space. The rationale for this action is that even with compression the feeds may take up a large amount of space, and bucket
    storage is an order of magnitude cheaper than server storage. (At time of writing, Jan 5 2018, Digital Ocean's entry level
    server is $5/month and has 20GB of space; its entry level bucket storage option is also $5/month and has 250GB of space.)
    
    To use the archive action, initialize in the common way for all actions:

        action = ArchiveAction(root_dir=, feeds=, quiet=, log_file_path=)

    see the action class for details on the arguments here. After this initialization it is necessary to set the bucket storage settings.
    These settings will be used by the Python boto3 package to interact with the storage. There are two attributes that need to be set. 
    The first is a dictionary which will be passed directly to the boto3.session.Session().client() in order to establish the connection.
    The exact form of this will depend on the bucket storage provided; the example below is for Digital Ocean spaces.
    The second attribute is the name of the bucket.

        action.boto3_settings = {
            'service_name' : 's3',
            'region_name' : 'nyc3',
            'endpoint_url' : 'https://nyc3.digitaloceanspaces.com',
            'aws_access_key_id' : '[Get from Digital Ocean control panel]',
            'aws_secret_access_key' : '[Get from Digital Ocean control panel]'
            }
        action.bucket = 'my_bucket'     

    Additional initialization may be desired by setting the limit attribute:

        action.limit = 10

    The action is then run using the run() method:

        action.run()

    Attributes:
        limit (int): an integer imposing an upper bound on the number of compressed files to transfer to storage. If set to -1, no limit is imposed. Default is -1.
        n_uploaded (int): the number of compressed files uploaded successfully.
        n_failed (int): the number of compressed files that failed to upload.
    """

    def run(self):
        """Run the archive action."""
        # Initialize variables and start the boto3 session
        self.n_uploaded = 0
        self.n_failed = 0
        self.output('Beginning archive action.')
        session = boto3.session.Session()
        client = session.client(**self.boto3_settings)

        # Iterate over all archives in the compressed directory
        files = glob.iglob(self.root_dir + settings.compressed_dir + '*/*/*.tar.bz2')    
        for file_name in files:
            # Check the last modification time; if it was within file_access_lag seconds ignore this file to avoid file I/O clash with a download action
            if(time.time()-os.path.getmtime(file_name) < self.file_access_lag):
                continue

            # Extract the date and time from the file_name
            # Use these to create the target_path in the storage
            a = file_name.rfind('/')
            b = file_name.find('-',a)
            c = file_name.find('.',b)
            utc = file_name[b+1:c] + '0000Z'
            date = file_name[b+1:b+11]
            hour = file_name[b+12:b+14]
            target_path = self.global_prefix + self.local_prefix + date + '/' + hour + '/' + file_name[a+1:] 

            # Now move to upload.
            # It may be possible that a file already exists in storage with the same name; in this case that archive and
            # the local archive need to be merged. 
            # First calculate the md5 hash of the local file, we'll need this anyway. Then see if the target_path is already
            # in storage. If it is, it may be just a duplicate (for instance, if two aggregating instances are running on 
            # separate machines. The md5 hash will tell us that. If they're different, we need to download the remote version
            # and merge it with the local version.
            md5 = tools.filesys.md5sum(file_name)
            perform_upload = True
            self.log.write('Preparing to upload ' + file_name)
            self.log.write('    target: ' + target_path)
            self.log.write('    md5: ' + md5)
            try:
                response = client.head_object(Bucket=self.bucket, Key=target_path)
                existing_md5 = response['ResponseMetadata']['HTTPHeaders']['x-amz-meta-md5chksum']
                self.log.write('    The target path exists already; its md5 hash is ' + existing_md5)
                if md5 != existing_md5:
                    # One can't directly merge bz2 files. We create a temporary directory, extract both archives into it, and then
                    # make one archive from the files inside.
                    temp_directory = 'tmp/' + utc + '/'
                    tools.filesys.ensure_dir(temp_directory)
                    client.download_file(self.bucket, target_path, temp_directory + 'existing.tar.bz2')
                    tools.filesys.tar_file_to_directory(temp_directory + 'existing.tar.bz2', temp_directory)
                    tools.filesys.tar_file_to_directory(file_name, temp_directory)
                    tools.filesys.directory_to_tar_file(temp_directory, file_name)
                    self.output('Target file existed in storage with different md5 hash: merged remote version with local version.')
                    self.log.write('    The md5s are different: merged local and remote versions.')
                else:
                    # If the md5s are the same, no need to actually do the upload
                    perform_upload = False

            except botocore.exceptions.ClientError:
                pass    

            # Perform the upload itself
            try:
                if perform_upload is True:
                    client.upload_file(file_name,self.bucket, target_path, ExtraArgs = { "Metadata": {"md5chksum": md5} })
            except Exception as e:
                self.n_failed += 1
                self.log_and_output('Failed to upload ' + file_name)
                self.log.write('Exception encountered follows')
                self.log.write(str(e))
                continue
        
            self.n_uploaded += 1
            os.remove(file_name)
            self.log.write('    success; original file deleted')
            self.output('Uploaded ' + file_name + ' -> ' +target_path)

            # Check if the processing limit has been reached
            if self.limit >= 0 and self.n_uploaded + self.n_failed >= self.limit:
                self.log_and_output('Reached file processing limit.')
                break

    
        total = tools.filesys.prune_directory_tree(self.root_dir + settings.compressed_dir)
        self.log.write('Removed ' + str(total) + ' directories in the compressed store.')
        self.log_and_output('Successfully uploaded ' + str(self.n_uploaded) + ' files')
        self.log_and_output('Failed to upload ' + str(self.n_failed) + ' files')

