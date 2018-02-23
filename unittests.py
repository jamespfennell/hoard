
import time

class VirtualDownloader():
    """Used to simulate downloading a remote feed."""

    def __init__(self, clock_init = time.time(), clock_increment = 0, ):

        self.clock_increment = clock_increment
        self.set_bad_download_ratios(0, 0, 0)
        self.set_duplicate_ratio(0)


    def set_bad_download_ratios(self, corrupt, empty, failed):
        assert corrupt >=0 and empty >= 0 and failed >= 0 and corrupt + empty + failed <= 1
        self.corrupt = corrupt
        self.empty = empty
        self.failed = failed
        self.regular = 1 - corrupt - empty - failed

    def set_duplicate_ratio(self, ratio):
        assert ratio >= 0 and ratio <= 1
        self.duplicate_ratio = ratio

    def copy(self, url, file_path):
        pass

