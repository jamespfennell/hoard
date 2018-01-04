from . import filesys
import os

class LatestTimeTracker():

    def __init__(self, directory):
        self.directory = directory
        filesys.ensure_dir(directory)


    def latest_time(self):
        latest = -1
        for timestamp in os.listdir(self.directory):
            if int(timestamp)>latest:
                latest = int(timestamp)
        return latest

    def add_time(self, timestamp):
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

