"""Provides the compress task class."""

import glob
import os
from .common import settings
from .common import task
from .common import log_templates_pb2
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
        num_compressed (int): number of compressed files created.
        num_hours (int): number of clock hours considered in this compression
                       task.
    """
    def __init__(self, **kwargs):

        super().__init__(**kwargs)
        # Initialize the log directory
        self._init_log('compress', log_templates_pb2.CompressTaskLog)

        # Initialize task configuration
        self.limit = -1
        self.file_access_lag = 120
        self.compress_all = False


    def _run(self):
        """Run the compress task."""
        self.num_compressed = 0
        self.num_hours = 0

        # Place the task configuration in the log
        self._log_run_configuration()
        self._log.limit = self.limit
        self._log.file_access_lag = self.file_access_lag
        self._log.compress_all = self.compress_all

        # Iterate over each hour of filtered files
        files = glob.glob(
            os.path.join(
                self._feeds_root_dir,
                'filtered',
                '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/[0-9][0-9]'
            )
            )
        for source_root_dir in files:
            # If compressall is off, check for the compress flag.
            # The compress flag is simply an empty file entitled 'compress'
            # in the hour's directory.
            cond1 = not self.compress_all
            cond2 = not os.path.isfile(
                os.path.join(source_root_dir, 'compress')
                )
            if cond1 and cond2:
                continue

            # Read the date and hour from the directory string
            i2 = source_root_dir.rfind('/')
            i1 = source_root_dir.rfind('/', 0, i2)
            date = source_root_dir[i1+1:i2]
            hour = source_root_dir[i2+1:]
            self.num_hours += 1

            # Create the directory into which all of the compressed files
            # will be put
            target_dir = os.path.join(
                self._feeds_root_dir,
                'compressed',
                date,
                hour
                )
            tools.filesys.ensure_dir(target_dir)

            hour_log = self._log.CompressedHour()
            hour_log.target_directory = target_dir

            # Now iterate over each of the uids, and compress the files in each
            for feed in self.feeds:
                feed_id = feed[0]
                source_dir = os.path.join(
                    source_root_dir,
                    feed_id
                    )
                target_file_name = '{}-{}T{}.tar.bz2'.format(
                    feed_id,
                    date,
                    hour
                    )
                target_file_path = os.path.join(
                    target_dir,
                    target_file_name
                    )
                #self.log.write('Compressing files in ' + source_dir)
                #self.log.write('    into ' + target_file)
                hour_log.target_file_names.append(target_file_name)
                if not os.path.isdir(source_dir):
                    hour_log.md5_hash.append('')
                    hour_log.num_compressed.append(0)
                    hour_log.source_directories.append(source_dir)
                    hour_log.appended.append(False)
                    continue

                # If the tar file already exists, extract it into the
                # filtered directory first
                # The cumulative effect will be that the new filtered files
                # will be `appended' to the tar file
                hour_log.num_compressed.append(
                    len([name for name in os.listdir(source_dir)])
                    )
                if os.path.isfile(target_file_path):
                    #self.log.write(
                    #        'File {} already exists; '.format(target_file) +
                    #        'already exists; extracting it first')
                    tools.filesys.tar_file_to_directory(
                        target_file_path,
                        source_dir
                        )
                    hour_log.appended.append(True)
                else:
                    hour_log.appended.append(False)

                hour_log.source_directories.append(source_dir)
                # Compress the source directory into the archive
                tools.filesys.directory_to_tar_file(
                    source_dir,
                    target_file_path
                    )
                hour_log.md5_hash.append(
                    tools.filesys.md5_hash(target_file_path)
                    )
                #self.log.write('Compressed')
                self.num_compressed += 1

            self._log.compressed_hours.extend([hour_log])
            # Delete the compress flag
            try:
                os.remove(os.path.join(source_root_dir, 'compress'))
            except FileNotFoundError:
                pass
            #self.log_and_output('Compressed hour: ' + date + 'T' + hour)

            # See if the compression limit has been reached, if so, close
            if self.limit >= 0 and self.n_compressed >= self.limit:
                self._log.limit_reached = True
                self._print('Reached limit of number of compressions to do.')
                self._print('Run again to compress more feeds.')
                #self.log_and_output(
                #        'Reached compression limit of '
                #        '{} files; ending.'.format(self.limit))
                #self.output(
                #        'Run again to compress more hours')
                break

        # Housekeeping, and log the results.
        total = tools.filesys.prune_directory_tree(
            os.path.join(self._feeds_root_dir, 'filtered')
            )
        self._print('Compress task ended.')
        self._print(
            '  * Created {} compressed archives '.format(self.num_compressed) +
            'corresponding to {} hour(s).'.format(self.num_hours)
            )
