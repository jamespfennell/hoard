"""Provides the download action class."""

import requests
import time
from . import settings
from . import action
from . import tools

class DownloadAction(action.Action):
    """This class provides the mechanism for performing download actions.

    Every [frequency] seconds, the download action performs a download cycle. A cycle downloads a current copy of
    the feeds. The action then sleeps for an appropriate number of seconds before initiating a new cycle. Because each
    cycle involves non-trivial data transfer, each cycle generally takes a non-negligible amount of time. This time is factored
    into the frequency calculation so that download cycles are initiated at the right frequency independently of how long
    each cycle actually takes. The only problem is if the downloads take longer than the desired frequency, in which case cycles will 
    become delayed. A warning is printed to the log in this instance.

    The download action is stopped in one of two ways: when a duration of time ([duration]) has elapsed, or if there
    is a keyboard interrupt.

    To use the download action, initialize in the common way for all actions:

        action = DownloadAction(root_dir=, feeds=, quiet=, log_file_path=)

    see the action class for details on the arguments here. Additional initialization is likely desired by setting limit attribute:

        action.limit = 100

    The action is then run using the run() method:

        action.run()
    

    Attributes:
        frequency (float): a float describing how often to download the feeds, in seconds. Default is 30 seconds.
        duration (float): a float describing how long to run the action before closing, in seconds. Default is 900 seconds (15 minutes).
	n_cycles (int): number of download cycles performed.
	n_downloads (int): number of files downloaded
    """

    def run(self):
        """Run the download action."""
        self.n_cycles = 0
        self.n_downloads = 0
        self.start_time = time.time()
        self.log.write('Running download action.')
        self.log.write('Collecting every ' + str(self.frequency) + ' seconds for ' + str(self.duration) + ' seconds.')
        self.output('Running download action.')

        while(True):
            # Perform a download cycle
            cycle_start_time = time.time()
            self.cycle()

            # If the action has been running for longer than the required duration, end it
            if cycle_start_time - self.start_time >= self.duration:
                self.stop('elapsed time')
                return

            # Pause for for the next cycle.
            # Cycles are to happen self.frequency seconds apart
            # The next cycle, the (N+1)th cycle, should commence at self.start_time + self.frequency*N seconds
            # Calculate how long to pause until this time is reached
            time_remaining = self.start_time + self.frequency*self.n_cycles - time.time()
            if time_remaining > 0:
                time.sleep(time_remaining)
            else:
                self.log.write('WARNING: unable to download feeds at desired frequency because of slow download speeds.')
                self.log.write('    Desired frequency: ' + str(self.frequency) + ' seconds.')
                self.log.write('    Average download time per cycle: ' + str((time.time() - self.start_time)/self.n_cycles) + ' seconds.')
            self.log.write('')


    def cycle(self):
        """Perform one download cycle."""
        # Initialize the cycle
        downloads_this_cycle = 0
        self.n_cycles += 1
        self.log.write('Beginning download cycle ' + str(self.n_cycles))
        self.log.write('Time since start: ' + str( int((time.time()-self.start_time)*1000)/1000 ) )

        # Establish the target directory into which the feeds will be downloaded
        t = (year, month, day, hour, mins, secs) = tools.time.timestamp_to_data_list()
        file_time = tools.time.timestamp_to_utc_8601()
        target_dir = self.root_dir + settings.downloaded_dir + year + '-'+month+'-'+day + '/' + hour + '/'
        self.log.write('Downloading to directory ' + target_dir)
        tools.filesys.ensure_dir(target_dir)

        # Iterate through every feed and download it.
        # The try/except block here is intentionally broad: in the worst case, only the present download should be abandoned, the program
        # should continue on no matter what happens locally inside here.
        for (uid, url, ext, func) in self.feeds:
            target_sub_dir = target_dir + uid + '/'
            target_file_name = uid + '-' + file_time + '-dt.' + ext
            tools.filesys.ensure_dir(target_sub_dir)
            try:
                r = requests.get(url)
                f = open(target_sub_dir + target_file_name, 'wb')
                f.write(r.content)
                f.close()
                downloads_this_cycle += 1
            except Exception as e: 
                self.log.write('Failed to download feed with UID: ' + uid)
                self.log.write(str(e))

        # Log the cycle results
        self.n_downloads += downloads_this_cycle
        self.log.write('Download cycle ended with ' + str(downloads_this_cycle) + '/' + str(len(self.feeds)) + ' feeds successfully downloaded')
        self.output('Cycle ' + str(self.n_cycles) + ': ' + str(downloads_this_cycle) + '/' + str(len(self.feeds)) + ' feeds downloaded.')
        

    def stop(self, reason=''):
        """Stop the download action.
        
        In fact, in the two ordinary cases when the action is stopped (running time has exceeded self.duration or a there was a keyboard
        interrupt) the action will already have been stopped in the sense that no new download cycles will be scheduled. The remaining task
        is simply to log why the action has stopped.

        Args:
            reason (str): The reason the action is being stopped.
        """
        # Log the reason to stop
        self.log.write('Closed because of ' + reason + '.')
        self.output('Closed because of ' + reason + '.')

        # Log the run results
        self.log.write('Download action finished with ' + str(self.n_cycles) + ' download cycles and ' + str(self.n_downloads) + ' total downloads.')
        self.output('Download action finished with ' + str(self.n_cycles) + ' download cycles and ' + str(self.n_downloads) + ' total downloads.')




