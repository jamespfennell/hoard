"""Provides the download task class."""

import requests
import time
from . import task
from ..logs import log_templates_pb2


class Downloader():
    """This class is provides a mechanism for downloadeing remote resourses.

    The class is esentially a wrapper around the requests library get
    function. Its purpose is to make simulating remote downloading easier
    by enclosing the process of downloading in an interface. In the unit
    tests module the Downloader class is replaced by a VirtualDownloader
    class which has the same API.
    """

    def __init__(self):
        pass

    def download(self, url, file_path, file_system):
        """Copy the content located at url to file_path."""
        r = requests.get(url)
        file_system.write_to_file(r.content, file_path)


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
    cycles will become delayed. A warning is printed in this instance.
    instance.

    The download task is stopped in one of two ways: when a duration of time
    ([duration]) has elapsed, or if there is a keyboard interrupt.

    To use the download task, initialize in the common way for all tasks:

        with DownloadTask(feeds=, storage_dir=) as task:
            # further initialization

    see the task class for details on the arguments here.
    Additional initialization is likely desired by setting the
    frequency and duration attributes:

            task.frequency = 30
            task.duration = 1000

    The task is then run using the run() method:

            task.run()

    Attributes:
        frequency (float): a float describing how often to download the
                           feeds, in seconds. Default is 30 seconds.
        duration (float): a float describing how long to run the task
                          before closing, in seconds. Default is 900
                          seconds (15 minutes).
        num_cycles (int): number of download cycles performed.
        num_downloaded_by_feed_url:
            (dictionary feed_url:int): the number of files downloaded
            from each url
    """

    def __init__(self, **kwargs):

        super().__init__(**kwargs)

        # Initialize the log directory
        self._init_log('download', log_templates_pb2.DownloadTaskLog)

        # Initialize the external components
        self._downloader = Downloader()

        # Initialize task configuration
        self.frequency = 14
        self.duration = 900

    def _run(self):
        """Run the download task."""

        self.start_time = self.time()

        # Place the task configuration in the log
        self._log_run_configuration()
        self._log.frequency = self.frequency
        self._log.duration = self.duration

        # When the task is complete, these variables will contain information
        # that will be be written to the log
        self._target_dirs = set()
        self.num_cycles = 0
        self.num_downloaded_by_feed_id = {feed[0]: 0 for feed in self.feeds}

        # Now, repeatedly perform download cycles.
        while(True):
            cycle_start_time = self.time()
            self.perform_cycle()

            # If the task has been running for longer than the required
            # duration, end it here.
            if self.time() - self.start_time >= self.duration:
                return

            # Otherwise, pause for for the next cycle.
            # Cycles are to happen self.frequency seconds apart
            # The next cycle, the (N+1)th cycle, should commence at
            # (self.start_time + self.frequency*N) seconds
            # First calculate how long to pause until this time is reached
            time_remaining = (
                self.start_time
                + self.frequency*self.num_cycles
                - self.time()
                )

            # Then pause, if necessary.
            # If no pause is needed downloads are taking longer than
            # the frequency, which is probably not desired.
            if time_remaining > 0:
                self._print(
                    'Sleeping for {:.2} seconds.'.format(time_remaining)
                    )
                time.sleep(time_remaining)
            else:
                avg_download_time = (
                    (self.time() - self.start_time)
                    / self.num_cycles
                    )
                self._print(
                    'WARNING: unable to download feeds at desired '
                    'frequency because of slow download speeds.'
                    )

    def perform_cycle(self):
        """Perform one download cycle."""

        # Initialize the cycle
        cycle_start_time = self.time()
        self.num_cycles += 1
        num_downloads = 0

        # Establish the target directory into which the feeds will
        # be downloaded
        target_dir = self._files_schema.downloaded_hour_dir(cycle_start_time)
        self._target_dirs.add(target_dir)

        # Start the cycle log
        cycle_log = self._log.DownloadCycleLog(
            start_time=int(cycle_start_time)
            )

        # Iterate through every feed and download it.
        # The try/except block here is intentionally broad:
        # in the worst case, only the present download should be abandoned,
        # the program should continue on no matter what happens locally
        # inside here.
        for (feed_id, feed_url, feed_ext, _) in self.feeds:
            # For the moment, pretend the download was succesful
            # If there's a problem, we will edit appropriately
            self.num_downloaded_by_feed_id[feed_id] += 1
            num_downloads += 1
            downloaded = True

            # Construct the target file path, and ensure the dir exists.
            target_file_path = self._files_schema.downloaded_file_path(
                cycle_start_time,
                feed_id,
                feed_ext
                )
            # Try to perform the download.
            # If there's an error, log it
            try:
                self._downloader.download(
                    feed_url,
                    target_file_path,
                    self._file_system
                    )
            except Exception as e:
                # The exception needs to be logged
                self._print('Failed to download feed with ID: ' + feed_id)
                self._print(str(e))
                task.populate_exception_log(cycle_log, e)

                # Decrement the downloaded count, and mark downloaded false
                self.num_downloaded_by_feed_id[feed_id] -= 1
                num_downloads -= 1
                downloaded = False

            # Log whether the download succeeded
            cycle_log.downloaded.append(downloaded)

        # Log the cycle results
        self._log.download_cycles.extend([cycle_log])
        self._print(
            'Completed download cycle with {}/{} succesful downloads.'.format(
                num_downloads,
                len(self.feeds)
                )
            )

    def _close(self):
        # Write various data to the log
        self._log.num_cycles = self.num_cycles
        for target_dir in self._target_dirs:
            self._log.target_directories.append(target_dir)
        for feed in self.feeds:
            self._log.num_downloaded.append(
                self.num_downloaded_by_feed_id[feed[0]]
                )

        total_num_downloads = sum(self.num_downloaded_by_feed_id.values())
        rate = 100*total_num_downloads / (self.num_cycles * len(self.feeds))
        self._print('Download task ended. Statistics:')
        self._print(' * {} download cycles.'.format(self.num_cycles))
        self._print(' * {} total downloads.'.format(total_num_downloads))
        self._print(' * {}% download success rate.'.format(int(rate)))
