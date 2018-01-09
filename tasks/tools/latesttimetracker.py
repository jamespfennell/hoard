"""Provides a class to track the latest time an event has taken place among multiple processes."""

import os
from . import filesys

class LatestTimeTracker():
    """This class provides a mechanism to track the latest time an event has taken place among multiple processes.

    We will illustrate the functionality with a contrived example. Suppose that you have an external thermometer which
    is checked every minute by a script called by Cron, and you are interested in keeping track of the last time the thermometer
    registered above 40 degrees Celsius. If the script was always running this would be easy, as the last time could be stored
    as a variable in the code, however the script is relaunched every minute. The last time therefore needs to be stored on disk.
    There needs to be a mechanism for updating the time on disk when above 40 is registered again, and a mechanism for reading the 
    current latest time on disk when you want it. This class provides the functionality.

    The class works with storage in a given directory. Times are recorded by creating empty files in this directory with
    file names equal to the timestamp. To read the latest time, we simply find the file with the largest timestamp.
    To add a new time, we simple touch the file 'directory/current_timestamp'. In the later case we can also delete smaller
    times to avoid redundant use of space.
    
    Note that this method avoids file I/O clashes if multiple instances are tracking the same time. No file is *really* opened
    for reading or writing, the information is transmitted entirely through the directory listing. The touch operation is a write operation,
    but if two processes try to do this simultaneously it's not a problem: the file will just be created by one of them, and the other
    will not do anything.
    """

    def __init__(self, directory):
        """Initialize a new latest time tracking instance.

        Arguments:
            directory (str): the location of the directory to be used to store timestamps.
        """
        self.directory = directory
        filesys.ensure_dir(directory)


    def latest_time(self):
        """Return the latest time on record. If there is no time on record, return -1."""
        latest = -1
        for timestamp in os.listdir(self.directory):
            if int(timestamp)>latest:
                latest = int(timestamp)
        return latest

    def add_time(self, timestamp):
        """Add a time to the record, and record the latest time on record.

        Arguments:
            timestamp (int): a Unix timestamp representing the time to be added to the record.
        """
        latest = self.latest_time()
        if latest >= timestamp:
            return latest
        else:
            # Mark that timestamp is the latest.
            try:
                open(self.directory + str(timestamp), 'x')
            except FileExistsError:
                pass

            # Delete other timestamps in the storage
            for existing_timestamp in os.listdir(self.directory):
                if int(existing_timestamp)<timestamp:
                    os.remove(self.directory + existing_timestamp)

            return timestamp

