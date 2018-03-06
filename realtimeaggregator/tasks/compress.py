"""Provides the compress task class."""

import os
from . import task
from ..logs import log_templates_pb2


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

        # These variables will be populated during the run
        self.num_compressed = 0
        self.num_hours = 0

    def _run(self):
        """Run the compress task."""

        # Place the task configuration in the log
        self._log_run_configuration()
        self._log.limit = self.limit
        self._log.file_access_lag = self.file_access_lag
        self._log.compress_all = self.compress_all

        # Iterate over each hour of filtered files.
        iterator = self._files_schema.list_filtered_hours(
            self.time(), self.compress_all
            )
        for (hour_dir, timestamp) in iterator:
            target_dir = self._files_schema.compressed_hour_dir(timestamp)

            # Begin the hour log.
            hour_log = self._log.CompressedHour()
            hour_log.target_directory = target_dir
            hour_log.timestamp = timestamp
            self.num_hours += 1

            # Iterate over each feed in the hour.
            iterator_2 = self._files_schema.list_feeds_in_filtered_hour(
                self.feeds, hour_dir
                )
            for ((feed_id, _, _, _), source_dir) in iterator_2:
                target_file_path = self._files_schema.compressed_file_path(
                    timestamp,
                    feed_id
                    )
                hour_log.target_file_names.append(
                    os.path.basename(target_file_path)
                    )

                # If the source_dir is None, there are no filtered feeds
                # for this feed, so skip.
                if source_dir is None:
                    hour_log.md5_hash.append('')
                    hour_log.num_compressed.append(0)
                    hour_log.source_directories.append('')
                    hour_log.appended.append(False)
                    continue

                # In this case files will be compressed.
                # Count the number of files that will be compressed, and
                # also log the source directory.
                hour_log.num_compressed.append(
                    self._file_system.num_files_in_dir(source_dir)
                    )
                hour_log.source_directories.append(source_dir)

                # If the tar file already exists, extract it into the
                # filtered directory first.
                # The cumulative effect will be that the new filtered files
                # will be `appended' to the tar file
                if self._file_system.isfile(target_file_path):
                    self._file_system.tar_file_to_dir(
                        target_file_path,
                        source_dir
                        )
                    hour_log.appended.append(True)
                else:
                    hour_log.appended.append(False)

                # Compress the source directory into the archive
                self._file_system.dir_to_tar_file(
                    source_dir,
                    target_file_path
                    )
                hour_log.md5_hash.append(
                    self._file_system.md5_hash(target_file_path)
                    )
                self.num_compressed += 1

            # Add the hour log to the task log.
            self._log.compressed_hours.extend([hour_log])

            # See if the compression limit has been reached, if so, close
            if self.limit >= 0 and self.n_compressed >= self.limit:
                self._log.limit_reached = True
                self._print('Reached limit of number of compressions to do.')
                self._print('   Run again to compress more feeds.')
                break

        # Housekeeping, and log the results.
        self._file_system.prune_dir_tree(self._files_schema.filtered_root_dir)
        self._print('\n'.join([
            'Compress task ended.',
            '  * Created {} compressed archives '.format(self.num_compressed),
            '  * Corresponding to {} hour(s).'.format(self.num_hours)
            ]))
