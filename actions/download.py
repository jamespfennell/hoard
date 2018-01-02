from .shared_code import *
import requests
import os



class DownloadAction():

    def __init__(self, frequency, duration, directory, feeds, quiet, log_file_path = None):
        """Initialize a new download action instance."""
        # Place the variables in the object
        self.freq = frequency
        self.dur = duration
        self.dir = directory
        self.feeds = feeds
        self.quiet = quiet
        self.n_cycles = 0
        self.n_downloads = 0
        self.start_time = time.time()

        # Initialize the log file
        if log_file_path == None:
            log_file_path = directory + download_log_dir + 'download-' + timestamp_to_utc_8601() + '.log'
        self.log = Log(log_file_path)
        self.log.write('New download action instance')
        
        # Record the settings in the log file
        self.log.write('Current working directory: ' + os.getcwd())
        self.log.write('Collecting every ' + str(self.freq) + ' seconds for ' + str(self.dur) + ' seconds.')
        self.log.write('Storing in directory ' + self.dir + download_log_dir)
        self.log.write('Collecting the following ' + str(len(feeds)) + ' feeds:')
        for (uid, url, ext, func) in feeds:
            self.log.write(' - UID: ' + uid + '; from URL: ' + url)


    def run(self):
        """Run the download action."""
        if self.quiet is False:
            print('Running download action.')
        while(True):
            self.cycle()
            current_time = time.time()
            self.log.write('Download cycles completed: ' + str(self.n_cycles) + '; time since start: ' + str(int(-self.start_time +current_time)) + ' seconds.')
            self.log.write('')

            # If the module has been running for longer than the required duration, end it
            if current_time - self.start_time >= self.dur:
                self.stop('elapsed time')
                return

            # Pause for frequency seconds
            # If the input was self.freq + time.time() - current_time then the total cycle time would be closer to the desired frequency
            time.sleep(self.freq)


    def stop(self, reason=''):
        # Log the reason to stop
        self.log.write('Closed because of ' + reason + '.')
        if self.quiet is False:
            print('Closed because of ' + reason + '.')

        # Log the run results
        self.log.write('Download action finished with ' + str(self.n_cycles) + ' download cycles and ' + str(self.n_downloads) + ' total downloads.')
        if self.quiet is False:
            print('Download action finished with ' + str(self.n_cycles) + ' download cycles and ' + str(self.n_downloads) + ' total downloads.')

    def cycle(self):
        """Perform one download cycle."""
        # Initialize the cycle
        self.n_cycles += 1
        downloads = 0
        self.log.write('Beginning download cycle ' + str(self.n_cycles))

        # Establish the directory into which the feeds will be downloaded
        t = (year, month, day, hour, mins, secs) = timestamp_to_data_list()
        file_time = timestamp_to_utc_8601()
        root_dir = self.dir + downloaded_dir + year + '-'+month+'-'+day + '/' + hour + '/'
        self.log.write('Downloading to directory ' + root_dir)
        ensure_dir(root_dir)

        # Iterate through every feed and download it.
        # The try/except block here is intentionally broad: in the worst case, only the present download should be abandoned, the program
        # should continue on no matter what happens locally inside here.
        for (uid, url, ext, func) in self.feeds:
            ensure_dir(root_dir + uid + '/')
            try:
                f = open(root_dir + uid + '/' + uid + '-' + file_time + '-dt.' + ext, 'wb')
                r = requests.get(url)
                f.write(r.content)
                f.close()
                downloads += 1
            except Exception as e: 
                self.log.write('Failed to download feed with UID: ' + uid)
                self.log.write(str(e))

        # Log the cycle results
        self.n_downloads += downloads
        self.log.write('Download cycle ended with ' + str(downloads) + '/' + str(len(self.feeds)) + ' feeds successfully downloaded')
        if self.quiet is False:
            print('Cycle ' + str(self.n_cycles) + ': ' + str(downloads) + '/' + str(len(self.feeds)) + ' feeds downloaded.')
        




