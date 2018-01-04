from . import tools
import time
import os

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
        self.log = tools.logs.Log(log_file_path)
        self.log.write('+---------------------+')
        self.log.write('| New action instance |')
        self.log.write('+---------------------+')
        
        # Record the general settings in the log file
        self.log.write('Current working directory: ' + os.getcwd())
        self.log.write('Storing and logging in directory ' + self.root_dir)
        self.log.write('Collecting the following ' + str(len(feeds)) + ' feeds:')
        for (uid, url, ext, func) in feeds:
            self.log.write(' - UID: ' + uid + '; from URL: ' + url)
            self.log.write(' -     extention: ' + ext + '; timestamp function: ' + func.__name__ + '()')

        # Create defaults for some variables; these allow all actions to be run without further initialization
        self.frequency = 30
        self.duration = 900
        self.limit = -1
        self.file_access_lag = 60
        self.force_compress = False

    def output(self, message):
        """Print message to stdout, if quiet mode is not on."""
        if self.quiet is False:
            print('  ' + message)


    def log_and_output(self, message):
        """Print message to stdout, if quiet mode is not on, and write the message to the log."""
        self.output(message)
        self.log.write(message)
    



