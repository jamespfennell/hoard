from .shared_code import *
from . import settings

import glob
import re
import time
import os
from shutil import copyfile, rmtree
import tarfile


    


class CompressAction(Action):


    def run(self):
        self.n_processed = 0
        self.n_compressions = 0
        self.output('Running compress action.')
        self.log.write('Running compress action.')


        files = glob.glob(self.root_dir + settings.filtered_dir + '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/[0-9][0-9]')    

        for directory in files:
            if not os.path.isfile(directory + '/compress'):
                continue

            # Read the date and hour from the directory string
            filtered_subdir = directory + '/'
            i2 = directory.rfind('/')
            i1 = directory.rfind('/',0,i2)
            date = directory[i1+1:i2]
            hour = directory[i2+1:]
            self.log.write('Potentially compressing files corresponding to hour ' + date + 'T' + hour)

            # Create the directory into which all of the compressed files will be put
            compressed_subdir = self.root_dir + settings.compressed_dir + date + '/' + hour + '/'
            ensure_dir(compressed_subdir)

            # Now iterate over each of the uids, and compress the files in each
            for uid in self.uids:
                source_dir = filtered_subdir + uid + '/'
                target_file = compressed_subdir + uid + '-' + date + 'T' + hour + '.tar.bz2'
                self.log.write('Compressing files in ' + source_dir)
                self.log.write('    into ' + target_file)
                if not os.path.isdir(source_dir):
                    continue
                 
                
                # If the tar file already exists, extract it into the filtered directory first
                # The cumulative effect will be that the new filtered files will be `appended' to the tar file
                if os.path.isfile(target_file):
                    self.log.write('File ' + target_file + ' already exists; extracting it first')
                    tar_file = tarfile.open(target_file, 'r:bz2')
                    tar_file.extractall(source_dir)
                    tar_file.close()
                    os.remove(target_file)

                # Compress the source directory into the archive
                tar_file = tarfile.open(target_file, 'x:bz2')
                tar_file.add(source_dir, arcname='')
                tar_file.close()
                self.n_compressions += 1
                self.log.write('Compressed')

                # Delete the source directory and its contents
                rmtree(source_dir)
                self.log.write('Removed directory ' + source_dir)

            

            os.remove(directory + '/compress')
            self.output('Compressed hour: ' + date + 'T' + hour)
            # See if the compression limit has been reached, if so, close
            if self.limit >= 0 and self.n_compressions >= self.limit:
                self.log.write('Reached compression limit of ' + str(self.limit) + ' files, ending')
                self.output('Reached compression limit of ' + str(self.limit) + ' files, ending')
                self.output('Run again to compress more hours')
                break


        remove_empty_directories(self.root_dir + settings.filtered_dir)
        self.log.write('Compressed files corresponding to ***' + str(self.n_compressions) + '*** hour(s)')
