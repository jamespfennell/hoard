from . import filesys
from . import time

class Log():

    def __init__(self, file_path):
        """Initialize a new log file.
        
        Keyword arguments:
        file_path -- the log file location"""
        # Ensure the directory exists, and then start the log file
        file_dir = file_path[:file_path.rfind('/')+1]
        filesys.ensure_dir(file_dir)
        self.log_ref = open(file_path, 'a', 1)
        self.last_time = None

    def __del__(self):
        self.log_ref.close()

    def write(self, text):
        """Prepend the current date and time to the string text, and then write (append) the string to the file given by file reference log.

        Keyword arguments:
        log -- a file reference to the log file
        text -- the string to write
        """
        # Calculate the datetime string
        p = time.timestamp_to_data_list()
        t = p[0] + '/' + p[1] + '/' + p[2] + ' ' + p[3] + ':' + p[4] + ':' + p[5]
        # If the time has changed since the last log write, add it.
        if t != self.last_time:
            pre = '[' + t + '] '
            self.last_time = t
        else:
            pre = '[                   ] '
        # Append the complete string to the log file
        self.log_ref.write(pre + text + '\n')
