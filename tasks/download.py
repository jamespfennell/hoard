"""Provides the download task class."""

import requests
import time
from .common import settings
from .common import task
from . import tools


class RemoteDownloader():
    """Provides a facility for downloading remote files."""

    def __init__(self):
        pass

    def copy(self, url, file_path):
        """Copy the content located at url to file_path."""
        r = requests.get(url)
        with open(file_path, 'wb') as f:
            f.write(r.content)


class DownloadTask(task.Task):
    """This class provides the mechanism for performing download tasks.

    Every [frequency] seconds, the download task performs a download cycle.
    A cycle downloads a current copy of the feeds. The task then sleeps for
    an appropriate number of seconds before initiating a new cycle. Because
    each cycle involves non-trivial data transfer, each cycle generally takes
    a non-negligible amount of time. This time is factored into the frequency
    calculation so that download cycles are initiated at the right frequency
    independently of how long each cycle actually takes. The only problem is
    if the downloads take longer than the desired frequency, in which case
    cycles will become delayed. A warning is printed to the log in this
    instance.

    The download task is stopped in one of two ways: when a duration of time
    ([duration]) has elapsed, or if there is a keyboard interrupt.

    To use the download task, initialize in the common way for all tasks:

        task = DownloadTask(root_dir=, feeds=, quiet=, log_file_path=)

    see the task class for details on the arguments here. Additional
    initialization is likely desired by setting limit attribute:bs

        task.limit = 100

    The task is then run using the run() method:

        task.run()

    Attributes:
        frequency (float): a float describing how often to download the
                           feeds, in seconds. Default is 30 seconds.
        duration (float): a float describing how long to run the task
                          before closing, in seconds. Default is 900
                          seconds (15 minutes).
        n_cycles (int): number of download cycles performed.
        n_downloads (int): number of files downloaded
    """

    def __init__(self, **args):
        task.Task.__init__(self, **args)
        self.remote = RemoteDownloader()

    def run(self):
        """Run the download task."""
        self.n_cycles = 0
        self.n_downloads = 0
        self.start_time = time.time()
        self.log.write('Running download task.')
        self.log.write(
                'Collecting every {}'.format(self.frequency) +
                ' seconds for {} seconds.'.format(self.duration))
        self.output('Running download task.')

        while(True):
            # Perform a download cycle through the self.cycle routine.
            cycle_start_time = time.time()
            self.cycle()

            # If the task has been running for longer than the required
            # duration, end it here.
            if cycle_start_time - self.start_time >= self.duration:
                self.stop('elapsed time')
                return

            # Pause for for the next cycle.
            # Cycles are to happen self.frequency seconds apart
            # The next cycle, the (N+1)th cycle, should commence at
            # self.start_time + self.frequency*N seconds
            # Calculate how long to pause until this time is reached
            time_remaining = (self.start_time + self.frequency*self.n_cycles
                              - time.time())
            if time_remaining > 0:
                time.sleep(time_remaining)
            else:
                avg_download_time = ((time.time() - self.start_time)
                                     / self.n_cycles)
                self.log.write('WARNING: unable to download feeds at '
                               'desired frequency because of slow '
                               ' download speeds.')
                self.log.write('    Desired frequency: '
                               '{} seconds.'.format(self.frequency))
                self.log.write('    Average download time per cycle: '
                               '{} seconds.'.format(avg_download_time))
            self.log.write('')

    def cycle(self):
        """Perform one download cycle."""
        # Initialize the cycle
        downloads_this_cycle = 0
        self.n_cycles += 1
        self.log.write('Beginning download cycle {}'.format(self.n_cycles))
        self.log.write('Time since start: '
                       '{:3f}'.format(time.time()-self.start_time))

        # Establish the target directory into which the feeds will
        # be downloaded
        t = tools.time.timestamp_to_data_list()
        (year, month, day, hour, mins, secs) = t
        file_time = tools.time.timestamp_to_utc_8601()
        target_dir = '{}{}{}-{}-{}/{}/'.format(
                self.root_dir, settings.downloaded_dir,
                year, month, day, hour)
        self.log.write('Downloading to directory ' + target_dir)
        tools.filesys.ensure_dir(target_dir)

        # Iterate through every feed and download it.
        # The try/except block here is intentionally broad:
        # in the worst case, only the present download should be abandoned,
        # the program should continue on no matter what happens locally
        # inside here.
        for (uid, url, ext, func) in self.feeds:
            target_sub_dir = target_dir + uid + '/'
            target_file_name = uid + '-' + file_time + '-dt.' + ext
            tools.filesys.ensure_dir(target_sub_dir)
            try:
                self.remote.copy(url, target_sub_dir + target_file_name)
                #r = requests.get(url)
                #f = open(target_sub_dir + target_file_name, 'wb')
                #f.write(r.content)
                #f.close()
                downloads_this_cycle += 1
            except Exception as e:
                self.log.write('Failed to download feed with UID: ' + uid)
                self.log.write(str(e))

        # Log the cycle results
        self.n_downloads += downloads_this_cycle
        self.log.write(
                'Download cycle ended with ' +
                '{}/{} '.format(downloads_this_cycle, len(self.feeds)) +
                'feeds successfully downloaded')
        self.output(
                'Cycle {}: '.format(self.n_cycles) +
                '{}/{}'.format(downloads_this_cycle, len(self.feeds)) +
                ' feeds downloaded.')

    def stop(self, reason=''):
        """Stop the download task.

        In fact, in the two ordinary cases when the task is stopped
        (running time has exceeded self.duration or a there was a keyboard
        interrupt) the task will already have been stopped in the sense
        that no new download cycles will be scheduled. The remaining task
        is simply to log why the task has stopped.

        Arguments:
            reason (str): The reason the task is being stopped.
        """
        # Log the reason to stop
        self.log.write('Closed because of {}.'.format(reason))
        self.output('Closed because of {}.'.format(reason))

        # Log the run results
        self.log.write(
                'Download task finished with ' +
                '{} download cycles and '.format(self.n_cycles) +
                '{} total downloads.'.format(self.n_downloads))
        self.output(
                'Download task finished with ' +
                '{} download cycles and '.format(self.n_cycles) +
                '{} total downloads.'.format(self.n_downloads))
