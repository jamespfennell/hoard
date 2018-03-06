"""Provides the task class, the template for all other task classes."""

import os
import time
import traceback
from . import files
from . import exceptions
from ..logs import log_templates_pb2


def populate_exception_log(exception, exception_log):
    """Write details about an exception to a log field."""

    exception_log.name = type(exception).__name__
    exception_log.text = str(exception)
    exception_log.traceback = traceback.format_exc()


class Task():
    """The task class is the template class for all other tasks classes.

    The task class handles basic initialization (principally setting variables
    and initiating a log) and provides the mechanism for printing to the log
    and to standard output. For a specific task, further initialization is
    probably desired in the form of setting relevant class attributes manually;
    which attributes are relevant depends on the specific task.

    Attributes:
        start_time (int): a Unix timestamp recording the start time.
        log (Log): a Log object printing to a log file, which
                   can be written using Log.write().
        uids (list(str)): a list of all user defined feed uids.
    """

    def __init__(self, feeds, storage_dir='', quiet=True):
        """Initialize a new task instance.

        Arguments:
            storage_dir (str): a string representing the directory in which
                            downloaded and processed files and logs will
                            be stored in. This can be given absolutely, or
                            relative to the current working directory.
            feeds (list(list)): a list of 4-tuples, where each tuple
                represents one feed. The tuples are of the form
                (feed uid, feed url, feed file extension, timestamp function).
                See remote_settings.py for detailed information on this tuple.
            quiet (bool): If set to True, standard output will be suppressed.
        """

        # Import arguments into the object
        self.feeds = feeds
        self._quiet = quiet
        self._feeds_root_dir = os.path.join(storage_dir, 'feeds')
        self._storage_dir = storage_dir
        if not hasattr(self, 'file_access_lag'):
            self.file_access_lag = 120

        # Initialize the two files interfaces: the schema, and through
        # it the files system object.
        self._files_schema = files.FilesSchema(
            self._storage_dir,
            self.file_access_lag
            )
        self._files_schema.set_feeds(feeds)
        self._file_system = self._files_schema.fs

        # Initialize some standard things
        self.time = time.time
        self._has_run = False

    def set_file_access_lag(self, new_lag):
            self.file_access_lag = new_lag
            self._files_schema._file_access_lag = new_lag

    def _init_log(self, log_name_prefix, LogObject):

        self._log_file_path = self._files_schema.log_file_path(
            self.time(),
            log_name_prefix
            )
        self._log = LogObject()

    def _log_run_configuration(self):
        pass

    def run(self):
        """Run the task."""

        # If the task has been run already, raise an exception.
        if self._has_run:
            raise exceptions.TaskAlreadyRanError()

        # Place the task configuration in the log
        self._log.start_time = int(self.time())
        self._log.working_directory = os.path.abspath('./')

        # Place feed information in the log
        for feed in self.feeds:
            self._log.feeds.extend([
                log_templates_pb2.FeedData(
                    feed_id=feed[0],
                    feed_url=feed[1],
                    feed_ext=feed[2],
                    feed_func_name=feed[3].__name__ + '()'
                    )
                ])

        # Try to run the task.
        # If there's a problem, log it.
        try:
            self._run()
        except BaseException as e:
            populate_exception_log(self._log.terminating_exception, e)
            self._print('Task ended with an exception.')
            raise e
        finally:
            # The close method allows individual tasks to do
            # specific clean up.
            self._close()

            # Write the log to disk.
            self._log.end_time = int(self.time())
            self._file_system.write_to_file(
                self._log.SerializeToString(),
                self._log_file_path
                )
            self._print('Log file: {}'.format(self._log_file_path))

        self._has_run = True

    def _close(self):
        pass

    def _run(self):
        pass

    def _print(self, text):
        """If not in quiet mode, print text."""

        if not self._quiet:
            print(text)
