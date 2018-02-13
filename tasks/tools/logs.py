"""Provides a mechanism for managing the creation and writing of log files."""

from . import filesys
from . import time


class Log():
    """This class provides a mechanism for creating log files and writing to them.

    A new log file is initialized when an object is created:

        log = Log('path/to/log.log')

    The log is written to using the write() method:

        log.write('An example message.')
        log.write('Another example message.')

    The write() method generally prepends the date and time to each message.
    However, if the date and time has not changed since the last write event,
    empty space is prepended instead to make the log more readable. The log
    file for the above commands will thus look something like this:

        [2018/01/04 19:30:58] An example message.
        [                   ] Another example message.
    """

    def __init__(self, file_path):
        """Initialize a new log file by opening the file.

        Keyword arguments:
            file_path (str): the log file location
        """
        # Ensure the directory exists, and then start the log file
        file_dir = file_path[:file_path.rfind('/')+1]
        filesys.ensure_dir(file_dir)
        self._log_ref = open(file_path, 'a', 1)
        self._last_time = None

    def __del__(self):
        self.close()

    def write(self, text):
        """Prepend the current date and time to the string text, and then
        write (append) the string to the file given by file reference log.

        Keyword arguments:
        log -- a file reference to the log file
        text -- the string to write
        """
        # Calculate the datetime string
        p = time.timestamp_to_data_list()
        t = '{}/{}/{} {}:{}:{}'.format(*p)
        # If the time has changed since the last log write, add it.
        if t != self._last_time:
            pre = '[{}]'.format(t)
            self._last_time = t
        else:
            pre = '[                   ] '
        # Append the complete string to the log file
        self._log_ref.write('{}{}\n'.format(pre, text))

    def close(self):
        """Close the log file."""
        self._log_ref.close()
