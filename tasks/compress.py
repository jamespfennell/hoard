"""Provides the compress task class."""

import glob
import os
from .common import settings
from .common import task
from . import tools


class CompressTask(task.Task):
    """This class provides the mechanism for performing compress tasks.

    Compress tasks are automatically scheduled when the downloads for a
    given clock hour have concluded. The compress task looks amongst all
    the filtered files, and groups files corresponding to the same feed and
    the given clock hour. It compresses these groups into a tar.bz2 file,
    places that file in the compressed directory, and then deletes the
    original files.

    To use the compress task, initialize in the common way for all tasks:

        task = CompressTask(root_dir=, feeds=, quiet=, log_file_path=)

    see the task class for details on the arguments here. Additional
    initialization is likely desired by setting the limit attribute:

        task.limit = 1000

    The task is then run using the run() method:

        task.run()

    Attributes:
        limit (int): an integer imposing an upper bound on the number of
                     compressed files to create. If set to -1, no limit is
                     imposed. Default is -1.
        compressall (bool): If True, will compress every clock hour
                            encountered in the filtered directory. If
                            False, will only compress those clock hours
                            for which all downloads have concluded.
                            Default is False.
        n_compressed (int): number of compressed files created.
        n_hours (int): number of clock hours considered in this compression
                       task.
    """

    def run(self):
        """Run the compress task."""
        self.n_compressed = 0
        self.n_hours = 0
        self.log_and_output('Running compress task.')

        # Iterate over each hour of filtered files
        files = glob.glob(
                self.root_dir +
                settings.filtered_dir +
                '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/[0-9][0-9]')
        for directory in files:
            # If compressall is off, check for the compress flag.
            # The compress flag is simply an empty file entitled 'compress'
            # in the hour's directory.
            cond1 = not self.compressall
            cond2 = not os.path.isfile(directory + '/compress')
            if cond1 and cond2:
                continue

            # Read the date and hour from the directory string
            filtered_subdir = directory + '/'
            i2 = directory.rfind('/')
            i1 = directory.rfind('/', 0, i2)
            date = directory[i1+1:i2]
            hour = directory[i2+1:]
            self.log.write('Potentially compressing files corresponding to '
                           'hour {}T{}.'.format(date, hour))
            self.n_hours += 1

            # Create the directory into which all of the compressed files
            # will be put
            compressed_subdir = ('{}{}{}/{}/'.format(
                    self.root_dir, settings.compressed_dir, date, hour))
            tools.filesys.ensure_dir(compressed_subdir)

            # Now iterate over each of the uids, and compress the files in each
            for uid in self.uids:
                source_dir = filtered_subdir + uid + '/'
                target_file = '{}{}-{}T{}.tar.bz2'.format(
                        compressed_subdir, uid, date, hour)
                self.log.write('Compressing files in ' + source_dir)
                self.log.write('    into ' + target_file)
                if not os.path.isdir(source_dir):
                    continue

                # If the tar file already exists, extract it into the
                # filtered directory first
                # The cumulative effect will be that the new filtered files
                # will be `appended' to the tar file
                if os.path.isfile(target_file):
                    self.log.write(
                            'File {} already exists; '.format(target_file) +
                            'already exists; extracting it first')
                    tools.filesys.tar_file_to_directory(
                            target_file, source_dir)

                # Compress the source directory into the archive
                tools.filesys.directory_to_tar_file(
                        source_dir, target_file)
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
                self.log_and_output(
                        'Reached compression limit of '
                        '{} files; ending.'.format(self.limit))
                self.output(
                        'Run again to compress more hours')
                break

        # Housekeeping, and log the results.
        total = tools.filesys.prune_directory_tree(
                self.root_dir + settings.filtered_dir)
        self.log.write(
                'Deleted {} subdirectories in '.format(total) +
                self.root_dir + settings.filtered_dir)
        self.log_and_output(
                'Created {} compress archives '.format(self.n_compressed) +
                'corresponding to {} hour(s).'.format(self.n_hours))
