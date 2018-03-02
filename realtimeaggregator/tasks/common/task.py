"""Provides the task class, the template for all other task classes."""

import time
import os
from .. import tools
from . import exceptions
from . import log_templates_pb2
import traceback


def exception_to_log(exception):
    return log_templates_pb2.ExceptionLog(
            name = type(exception).__name__,
            text = str(exception),
            traceback = traceback.format_exc()
            )


def add_exception_to_log(exception, exception_log):
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

    def __init__(self, feeds, storage_dir='./', quiet=True):
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
        tools.filesys.ensure_dir(self._feeds_root_dir)

        # Initialize some standard stuff
        self.time = time.time
        self._in_context = False
        self._has_run = False

    def _init_log(self, log_name_prefix, LogObject):

        # Initialize the log directory
        day_dir, hour_dir, file_time = tools.time.timestamp_to_path_pieces()
        self._log_dir = os.path.join(
            self._storage_dir, 
            'logs',
            day_dir,
            hour_dir
            )
        tools.filesys.ensure_dir(self._log_dir)
        self._log_file_path = os.path.join(
            self._log_dir,
            '{}-{}.log'.format(log_name_prefix, file_time)
            )    
        self._log = LogObject()

    def _log_run_configuration(self):
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
        

    def run(self):
        if self._has_run:
            raise exceptions.TaskAlreadyRanError()
        
        try:
            self._run()
        except:
            #exception_log = self._log.terminating_exception
            #exception_log.name = exception_obj.__name__
            #exception_log.text = str(exception_value)
            #exception_log.traceback = traceback.format_exc()
            self._print('Task ended with an exception.')
            raise

        finally:
            self._log.end_time = int(self.time())
            self._close()

            # Write the log to disk.
            with open(self._log_file_path, 'wb') as f:
                f.write(self._log.SerializeToString())

            self._print('Log file: {}'.format(self._log_file_path))



        self._has_run = True

    def _close(self):
        pass
                

    def _print(self, text):
        """If not in quiet mode, print text."""

        if not self._quiet:
            print(text)


