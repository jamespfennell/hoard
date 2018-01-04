from . import settings
from . import tools
from . import action

import glob
import re
import time
import os
from shutil import copyfile, rmtree
import tarfile


    


class CompressAction(action.Action):


    def run(self):
        self.n_compressed = 0
        self.n_hours = 0
        self.log_and_output('Running compress action.')

        # Iterate over each hour of filtered files
        files = glob.glob(self.root_dir + settings.filtered_dir + '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/[0-9][0-9]')    
        for directory in files:
            # If force_compress is off, check for the compress flag.
            # The compress flag is simply an empty file entitles 'compress' in the hour's directory.
            if self.force_compress == False and not os.path.isfile(directory + '/compress'):
                continue

            # Read the date and hour from the directory string
            filtered_subdir = directory + '/'
            i2 = directory.rfind('/')
            i1 = directory.rfind('/',0,i2)
            date = directory[i1+1:i2]
            hour = directory[i2+1:]
            self.log.write('Potentially compressing files corresponding to hour ' + date + 'T' + hour)
            self.n_hours += 1

            # Create the directory into which all of the compressed files will be put
            compressed_subdir = self.root_dir + settings.compressed_dir + date + '/' + hour + '/'
            tools.filesys.ensure_dir(compressed_subdir)

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
                    tools.filesys.tar_file_to_directory(target_file, source_dir)

                # Compress the source directory into the archive
                tools.filesys.directory_to_tar_file(source_dir, target_file)
                self.log.write('Compressed')
                self.n_compressed += 1


            
            # Delete the compress flag
            try:
                os.remove(directory + '/compress')
            except FileNotFoundError:
                pass
            self.log_and_output('Compressed hour: ' + date + 'T' + hour)

            # See if the compression limit has been reached, if so, close
            if self.limit >= 0 and self.n_compressed >= self.limit:
                self.log_and_output('Reached compression limit of ' + str(self.limit) + ' files, ending')
                self.output('Run again to compress more hours')
                break

        total = tools.filesys.prune_directory_tree(self.root_dir + settings.filtered_dir)
        self.log.write('Deleted ' + str(total) + ' subdirectories in ' + self.root_dir + settings.filtered_dir)
        self.log_and_output('Created ' + str(self.n_compressed) + ' compressed archives corresponding to ' + str(self.n_hours) + ' hour(s)')
