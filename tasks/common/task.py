"""Provides the task class, the template for all other task classes."""

import time
import os
from .. import tools


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

    def __init__(self, root_dir, feeds, quiet, log_file_path):
        """Initialize a new task instance.

        Arguments:
            root_dir (str): a string representing the directory in which
                            downloaded and processed files and logs will
                            be stored in. This can be given absolutely, or
                            relative to the current working directory.
            feeds (list(list)): a list of 4-tuples, where each tuple
                represents one feed. The tuples are of the form
                (feed uid, feed url, feed file extension, timestamp function).
                See remote_settings.py for detailed information on this tuple.
            quiet (bool): If set to True, standard output will be suppressed.
            log_file_path (str): a path for the log file.
        """

        # Place the variables in the object
        self.root_dir = root_dir
        self.feeds = feeds
        self.quiet = quiet
        self.start_time = time.time()
        self.uids = [feed[0] for feed in self.feeds]

        # Initialize the log file
        self.log = tools.logs.Log(log_file_path)
        self.log.write('+-------------------+')
        self.log.write('| New task instance |')
        self.log.write('+-------------------+')

        # Record the general settings in the log file
        self.log.write('Current working directory: ' + os.getcwd())
        self.log.write('Storing and logging in directory ' + self.root_dir)
        self.log.write('Collecting the following {} feeds:'.format(len(feeds)))
        for (uid, url, ext, func) in feeds:
            self.log.write(
                    ' - UID: {};'.format(uid) +
                    'from URL: {}.'.format(url))
            self.log.write(
                    ' -   extention: {};'.format(ext) +
                    ' timestamp function: ' +
                    '{}()'.format(ext, func.__name__))

        # Create defaults for some variables; these allow all tasks
        # to be run without further initialization
        self.frequency = 30
        self.duration = 900
        self.limit = -1
        self.file_access_lag = 60
        self.compressall = False

    def output(self, message):
        """Print message to stdout, if quiet mode is not on."""
        if self.quiet is False:
            print('  {}'.format(message))

    def log_and_output(self, message):
        """Print message to stdout, if quiet mode is not on,
        and write the message to the log."""
        self.output(message)
        self.log.write(message)
