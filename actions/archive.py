from .shared_code import *
from . import settings
import glob
import os
import time

import boto3
from botocore.client import Config
class ArchiveAction(Action):


    def run(self):
        self.n_uploaded = 0
        self.n_failed = 0
        if self.quiet is False:
            print('Beginning archive action.')

        session = boto3.session.Session()
        client = session.client(**self.boto3_settings)



        current_time = time.time()

        files = glob.iglob(self.root_dir + settings.compressed_dir + '*/*/*.tar.bz2')    

        for file_name in files:
            print(file_name)
            a = file_name.rfind('/')
            b = file_name.find('-',a)
            c = file_name.find('.',b)
            utc = file_name[b+1:c] + '0000Z'
            t = utc_8601_to_timestamp(utc)

            date = file_name[b+1:b+11]
            hour = file_name[b+12:b+14]
            
            #If this file was created more than 2 minutes ago, upload it
            if current_time-t > self.file_access_lag:
                path = file_name[file_name.find('/')+1:]
                md5 = md5sum(file_name)
                self.log.write('Uploading: ' + file_name)
                self.log.write('    md5: ' + md5)
                target_path = self.global_prefix + self.local_prefix + date + '/' + hour + '/' + file_name[a+1:] 
                self.log.write('    target: ' + target_path)
                try:
                    client.upload_file(file_name,self.bucket, target_path, ExtraArgs = { "Metadata": {"md5chksum": md5} })

                except Exception as e:
                    self.n_failed += 1
                    self.log.write('Error in upload; Exception follows')
                    self.log.write(str(e))
                    if self.quiet is False:
                        print('Failed to upload ' + file_name)
                    continue
            
                self.n_uploaded += 1
                os.remove(file_name)
                self.log.write('    success; original file deleted')
                self.output('Uploaded ' + file_name)


                if self.limit >= 0 and self.n_uploaded + self.n_failed >= self.limit:
                    self.output('Max number of uploads reached')
                    self.log.write('Max number of uploads reached')
                    break

        
        remove_empty_directories(self.root_dir + settings.compressed_dir)
        self.log.write('Successfully uploaded ' + str(self.n_uploaded) + ' files')
        self.log.write('Failed to upload ' + str(self.n_failed) + ' files')

