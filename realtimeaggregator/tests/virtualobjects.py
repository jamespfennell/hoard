"""Provides objects to emulate remote services.

Unit tests are generally designed to avoid file system interaction and
accesing remote services. This is to ensure that if the unit test fails, it
is because of an error in the software itself and not some temporary
problem with the remote services.

The aggregating software accesses two remote services: it downloads feeds
from a remote server and it uploads aggregated feeds to remote object storage.
It performs these actions through the Downloader and Boto3Transferer
objects respectively. This module contains two objects, VirtualDownloader
and VirtualTransferer, which have the same interface as the true objects,
but do not access remote resources.
"""

import hashlib
import random
import requests.exceptions
import string
import time
from . import utils


class VirtualDownloader():
    """Used to locally simulate downloading a remote feed."""

    def __init__(self, clock_init=time.time(), clock_increment=2.51):

        self.clock = clock_init
        self.clock_increment = clock_increment
        self.set_bad_download_ratios(0, 0, 0)

        # Initialize feed related varialbes
        self.feeds = []
        self.refresh_periods = {}
        self.last_refresh = {}
        self.responses = {}

    def set_bad_download_ratios(self, corrupt, empty, failed):
        """Set the proportion of bad feeds.

        The arguments must be positve and sum to something less than 1.
        The proportion of regular (valid) feeds will be 1 minues the sum
        of the arguments.
        """
        assert corrupt >= 0 and empty >= 0 and failed >= 0
        assert corrupt + empty + failed <= 1
        regular = 1 - corrupt - empty - failed
        self._weights = [regular, corrupt, empty, failed]

    def add_virtual_feed(self, refresh_period=15):
        """Add a virtual remote feed and return its virtual url.

        The refresh period dictates how often the feed will be 'updated'.
        Setting a longer refresh period will result in more duplicate
        feed downloads.
        """

        # Define the basic feed settings.
        feed_id = 'feed{}'.format(len(self.feeds)+1, time.time())
        feed_ext = 'unittest'
        feed_url = 'unittest://{}.{}'.format(feed_id, feed_ext)
        feed_func = utils.unittest_feed_reader
        self.feeds.append([feed_id, feed_url, feed_ext, feed_func])
        self.refresh_periods[feed_url] = refresh_period
        # The last refresh period is set to be a random time in the past.
        # This way, multiple feeds with the same refresh period will
        # have different refresh times.
        self.last_refresh[feed_url] = (
            self.clock
            - random.randrange(refresh_period)
            - refresh_period
            )
        # Initialize the responses counter.
        self.responses[feed_url] = {
            'valid': 0,
            'corrupt': 0,
            'empty': 0,
            'failed': 0,
            'unique': 0,
            }

        return feed_url

    def time(self):
        """Return the current time of the internal clock."""
        return self.clock

    def download(self, url, file_path, files_interface):
        """Download a copy of the virtual feed at url to file_path."""

        self.clock += self.clock_increment
        # The kind of feed to return is determined randomly
        action = random.choices(
            population=['valid', 'corrupt', 'empty', 'failed'],
            weights=self._weights
            )[0]
        # Increment the relevant counter
        self.responses[url][action] += 1
        if action == 'valid':
            # First determine the last refresh time.
            # This is calculated as the most recent time of the form
            # feed_start_time + n * refresh_period
            # First, how many refresh cycles should have taken place
            # since the last refresh_time
            num_refresh = int(
                (self.clock - self.last_refresh[url])
                / self.refresh_periods[url]
                )
            # Then add that much time to the last refresh time
            if num_refresh > 0:
                self.last_refresh[url] += (
                    num_refresh * self.refresh_periods[url]
                    )
                self.responses[url]['unique'] += 1

            response = str(int(self.last_refresh[url]))
        elif action == 'corrupt':
            # Return some random letters
            response = ''.join(random.choices(string.ascii_uppercase, k=20))
        elif action == 'empty':
            response = ''
        elif action == 'failed':
            # Simulate a requests library exception
            raise requests.exceptions.RequestException(
                'Simulated download error.'
                )

        files_interface.write_to_file(response.encode('utf-8'), file_path)

    def attach_to(self, task):
        """Replace the downloader in a task with this virtual downloader.

        As well as replacing the downloader object, this function also:
          * Imports the virtual feeds into the task,
          * Has the task read time from the virtual server,
          * Update the files schema with the new feeds.
        """
        task._downloader = self
        task.feeds = self.feeds
        task.time = self.time
        task._files_schema.set_feeds(task.feeds)


class VirtualTransferer():

    def __init__(self, error_rate=0):
        assert error_rate >= 0 and error_rate <= 1
        self._storage_dir = ''
        self.error_rate = error_rate
        self.num_download_errors = 0
        self.num_upload_errors = 0
        self.objects_handled = set()
        self._objects = {}
        self._md5_hashes = {}

    def _location(self, object_key):
        return hash(object_key)

    def upload(self, file_path, files_int, object_key, remove_original=False):
        self.objects_handled.add(self._location(object_key))
        if random.random() < self.error_rate:
            self.num_upload_errors += 1
            raise Exception('Simulated storage transferer upload error.')
        internal_key = self._location(object_key)
        self._objects[internal_key] = files_int.content(file_path)
        d = hashlib.md5()
        d.update(self._objects[internal_key])
        self._md5_hashes[internal_key] = d.hexdigest()

        if remove_original:
            files_int.remove(file_path)

    def download(self, object_key, file_path, files_int):
        self.objects_handled.add(self._location(object_key))
        if random.random() < self.error_rate:
            self.num_download_errors += 1
            raise Exception('Simulated storage transferer download error.')
        files_int.write_to_file(
            self._objects[self._location(object_key)],
            file_path
            )

    def md5_hash(self, object_key):
        self.objects_handled.add(self._location(object_key))
        try:
            return self._md5_hashes[self._location(object_key)]
        except KeyError:
            raise self.ObjectDoesNotExist

    def attach_to(self, task):
        task._transferer = self

    class ObjectDoesNotExist(Exception):
        pass
