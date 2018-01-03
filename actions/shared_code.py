"""
This file contains general code used by all four modules. It contains, in order:
    (1) Code to manage printing to the logs.
    (2) Code to deal with time, mostly converting between Unix timestamps and UTC 8601
    (3) Code to deal with some file I/O issues
    (4) Variables describing the directory structure of the program
"""

import time
import os
import calendar
import hashlib
from functools import partial

# (1) LOG PRINTING FACILITY

class Log():

    def __init__(self, file_path):
        """Initialize a new log file.
        
        Keyword arguments:
        file_path -- the log file location"""
        # Ensure the directory exists, and then start the log file
        file_dir = file_path[:file_path.rfind('/')+1]
        ensure_dir(file_dir)
        self.log_ref = open(file_path, 'w', 1)
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
        p = timestamp_to_data_list()
        t = p[0] + '/' + p[1] + '/' + p[2] + ' ' + p[3] + ':' + p[4] + ':' + p[5]
        # If the time has changed since the last log write, add it.
        if t != self.last_time:
            pre = '[' + t + '] '
            self.last_time = t
        else:
            pre = '[                   ] '
        # Append the complete string to the log file
        self.log_ref.write(pre + text + '\n')

# Variable to store the last time the log was written to
last_time = None

def print_to_log(log, text):
    """Prepend the current date and time to the string text, and then write (append) the string to the file given by file reference log.

    Keyword arguments:
    log -- a file reference to the log file
    text -- the string to write
    """
    # The last_time variable is used to see if it necessary to add the current date and time to the string.
    global last_time
    # Calculate the datetime string
    p = timestamp_to_data_list()
    t = p[0] + '/' + p[1] + '/' + p[2] + ' ' + p[3] + ':' + p[4] + ':' + p[5]
    # If the time has changed since the last log write, add it.
    if t != last_time:
        pre = '[' + t + '] '
        last_time = t
    else:
        pre = '[                   ] '
    # Append the complete string to the log file
    log.write(pre + text + '\n')


# (2) TIME BASED FUNCTIONS

def timestamp_to_utc_8601(timestamp = -1):
    """Given a unix timestamp, return the UTC 8601 time in the form YYYY-MM-DDTHHMMSSZ, where T and Z are constants
    and the remaining letters are substituted by the associated date time elements.

    Keyword arguments:
    timestamp -- a integer representing the time as a Unix timestamp. If -1, set equal to the current Unix time.
    """
    t = timestamp_to_data_list(timestamp)
    return t[0] + '-' + t[1] + '-' + t[2] + 'T' + t[3] + '' + t[4] + '' + t[5] + 'Z'

def utc_8601_to_timestamp(utc):
    """Given a UTC 8601 time, return the associated Unix timestamp.
    
    Keyword arguments:
    utc -- a UTC 8601 formatted string in the form YYYY-MM-DDTHHMMSSZ where T and Z are constants and the remaining letters 
            are substituted by the associated datetime elements.
    """
    # Read the datetime elements from the string
    year = int(utc[0:4])
    month = int(utc[5:7])
    day = int(utc[8:10])
    hour = int(utc[11:13])
    mins = int(utc[13:15])
    secs = int(utc[15:17])
    # Put the elements in the from of a time struct
    t = (year,month,day,hour,mins,secs,-1,-1,0)
    # Use calender to convert the time struct into a Unix timestamp
    return calendar.timegm(t)


def timestamp_to_data_list(timestamp = -1):
    """Return a 6-tuple of strings (year, month, day, hour, minute, second) representing the time given by the Unix timestamp.
    The year string has length exactly 4 and the other strings have length exactly 2, with left 0 padding if necessary to achieve this.

    Keyword arguments:
    timestamp -- a integer representing the time as a Unix timestamp. If -1, set equal to the current Unix time.
    """
    if timestamp == -1:
        now = time.gmtime()
    else:
        now = time.gmtime(timestamp)
    # Read the data from the time struct.
    soln = [str(now.tm_year), 
            str(now.tm_mon),
            str(now.tm_mday),
            str(now.tm_hour),
            str(now.tm_min),
            str(now.tm_sec)
        ]
    # Left pad with zeroes if necessary, and return.
    for k in range(1,6):
        if len(soln[k]) == 1:
            soln[k] = '0' + soln[k]
    return soln


# (3) FILE I/O FUNCTIONS

def md5sum(filename):
    with open(filename, mode='rb') as f:
        d = hashlib.md5()
        for buf in iter(partial(f.read, 128), b''):
            d.update(buf)
    return d.hexdigest()
def ensure_dir(path):
    """Ensure that the local directory given by path exists. If it does not exist, create it."""
    d = os.path.dirname(path)
    if not os.path.exists(d):
        os.makedirs(d)

def silent_delete_attempt(path):
    try:
        os.rmdir(path)
    except FileNotFoundError:
        pass



def remove_empty_directories(root):
    total = 0
    for subdir, dirs, files in os.walk(root, topdown = False):
        if subdir == root:
            continue
        if len(dirs) + len(files) == 0:
            os.rmdir(subdir)
            total += 1
    return total

# (4) INTERNAL SETTINGS

# The following variables describe the directory structure of the aggregated files and the log files

downloaded_dir = 'store/downloaded/'
download_log_dir = 'logs/download/'

filtered_dir = 'store/filtered/'
filter_log_dir = 'logs/filter/'

compressed_dir = 'store/compressed/'
compress_log_dir = 'logs/compress/'




# (5) ACTION

class Action():

    def __init__(self, root_dir, feeds, quiet, log_file_path):
        """Initialize a new action instance."""
        # Place the variables in the object
        self.root_dir = root_dir
        self.feeds = feeds
        self.quiet = quiet
        self.start_time = time.time()
        self.uids = [feed[0] for feed in self.feeds]

        # Initialize the log file
        self.log = Log(log_file_path)
        self.log.write('New action instance')
        
        # Record the general settings in the log file
        self.log.write('Current working directory: ' + os.getcwd())
        self.log.write('Working in root directory ' + self.root_dir)
        self.log.write('Collecting the following ' + str(len(feeds)) + ' feeds:')
        for (uid, url, ext, func) in feeds:
            self.log.write(' - UID: ' + uid + '; from URL: ' + url)
            self.log.write(' -     extention: ' + ext + '; timestamp function: ' + func.__name__ + '()')

        # Create defaults for some variables; these allow all actions to be run without further initialization
        self.frequency = 30
        self.duration = 900
        self.limit = -1
        self.file_access_lag = 60

    def output(self, message):
        """Print message to stdout, if quiet mode is not on."""
        if self.quiet is False:
            print(message)


    



