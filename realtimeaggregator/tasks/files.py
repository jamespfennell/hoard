"""
This module provides functionality relating to reading and writing files.

It contains two objects that have quite distinct functions. The FilesSchema
object is responsible for ensuring that a file naming convention is adhered
to across tasks. It provides functions that map a type of file ('downloaded
feed downloaded right now') to locations on the filesystem
('/feeds/downloaded/2018-03-03/12/feedid-20180303T120456-dt.feedext').
In also provides functions for listing types of files on the file system
('list all downloaded files'). These two functions have to be consistent:
the latter function iterates over paths of the type given by the first
function. By placing all of these functions in one place, it is easier
to maintain such consistency.

The second object FileSystem provides methods for interacting with the
file system. Many of these methods are simply standard methods from the os
or shutil packages. There are two motivations for FileSystem:

    (1) Simplify the process of working with files. This is achieved by
        automatically making directories if needed (for example, when copying
        a file) and also by bundling standard operations ('compress the
        contents of this directory into a tar.bz2 archive') into simple
        calls.

    (2) Allow the FileSystem method to be replaced by a VirtualFileSystem
        method when doing unit tests. This VirtualFileSystem would allow
        tasks to be unit tested without touching the underlying filesystem,
        as some people recomend. This has yet to be implemented.
"""

import functools
import glob
import hashlib
import os
import shutil
import tarfile
import time
import calendar


def _timestamp_to_iso_8601(timestamp=-1):
    """Given a unix timestamp, return the UTC 8601 time string in the form
    YYYY-MM-DDTHHMMSSZ, where T and Z are constants and the remaining letters
    are substituted by the associated date time elements.

    For example, timestamp_to_utc_8601(1515174235) returns
    '2018-01-05T174355Z', which corresponds to the time 17:43:55 on
    January 5th, 2018 (UTC time).

    Arguments:
        timestamp (int): an integer representing the time as a Unix timestamp.
                         If -1, set equal to the current Unix time.
    """
    t = _timestamp_to_data_list(timestamp)
    return '{}-{}-{}T{}{}{}Z'.format(*t)


def _iso_8601_to_timestamp(utc):
    """Given a UTC 8601 time string in the form YYYY-MM-DDTHHMMSSZ,
    return the associated Unix timestamp.

    Arguments:
        utc (str): a UTC 8601 formatted string in the form YYYY-MM-DDTHHMMSSZ
                   where T and Z are constants and the remaining letters
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
    t = (year, month, day, hour, mins, secs, -1, -1, 0)
    # Use calendar to convert the time struct into a Unix timestamp
    return calendar.timegm(t)


def _timestamp_to_data_list(timestamp=-1):
    """Return a 6-tuple of strings (year, month, day, hour, minute, second)
    representing the time given by the Unix timestamp. The year string will
    have length exactly 4 and the other strings will have length exactly 2,
    with left 0 padding if necessary to achieve this.

    Arguments:
        timestamp (int): a integer representing the time as a Unix timestamp.
                         If -1, set equal to the current Unix time.
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
    for k in range(1, 6):
        if len(soln[k]) == 1:
            soln[k] = '0' + soln[k]
    return soln


class FilesSchema():

    def __init__(self, root_dir, file_access_lag):

        self.fs = FileSystem()
        self.root_dir = root_dir
        self._file_access_lag = file_access_lag
        self.feeds = []
        self.feeds_by_id = {}

        self.feeds_dir = os.path.join(self.root_dir, 'feeds')
        self.logs_root_dir = os.path.join(self.root_dir, 'logs')
        self.downloaded_root_dir = os.path.join(self.feeds_dir, 'downloaded')
        self.filtered_root_dir = os.path.join(self.feeds_dir, 'filtered')
        self.compressed_root_dir = os.path.join(self.feeds_dir, 'compressed')

        self._datetime_sub_dirs_cache = {}

    def set_feeds(self, feeds):
        self.feeds = feeds
        self.feeds_by_id = {feed[0]: feed for feed in self.feeds}

    # (1) PART ONE: methods to return a filename given certain data.

    # (1.0) Utilify functions

    def _file_name(self, timestamp, feed_id, feed_ext, affix=''):
        return '{}-{}{}.{}'.format(
            feed_id,
            _timestamp_to_iso_8601(timestamp),
            affix,
            feed_ext
            )

    def _datetime_sub_dirs(self, timestamp):
        timestamp -= timestamp % 3600 - 1800
        if timestamp not in self._datetime_sub_dirs_cache:
            (year, month, day, hour, _, _) = _timestamp_to_data_list(timestamp)
            self._datetime_sub_dirs_cache[timestamp] = os.path.join(
                '{}-{}-{}'.format(year, month, day),
                hour
                )
        return self._datetime_sub_dirs_cache[timestamp]

    # (1.1) Log files

    def log_file_path(self, timestamp, prefix):
        """Return the file path of log file created at a certain time."""
        return os.path.join(
            self.logs_root_dir,
            self._datetime_sub_dirs(timestamp),
            '{}-{}.log'.format(prefix, _timestamp_to_iso_8601(timestamp))
            )

    # (1.2) Download task

    def downloaded_hour_dir(self, timestamp):
        return os.path.join(
            self.downloaded_root_dir,
            self._datetime_sub_dirs(timestamp)
            )

    def downloaded_dir(self, timestamp, feed_id):
        return os.path.join(
            self.downloaded_hour_dir(timestamp),
            feed_id
            )

    def downloaded_file_path(self, timestamp, feed_id, feed_ext):
        return os.path.join(
            self.downloaded_dir(timestamp, feed_id),
            self._file_name(timestamp, feed_id, feed_ext, '-dt')
            )

    # (1.3) Filter task

    def filtered_dir(self, timestamp, feed_id):
        return os.path.join(
            self.filtered_root_dir,
            self._datetime_sub_dirs(timestamp),
            feed_id
            )

    def filtered_file_path(self, timestamp, feed_id, feed_ext):
        return os.path.join(
            self.filtered_dir(timestamp, feed_id),
            self._file_name(timestamp, feed_id, feed_ext)
            )

    # (1.4) Compress task

    def compressed_hour_dir(self, timestamp):
        timestamp = timestamp - timestamp % 3600
        return os.path.join(
            self.compressed_root_dir,
            self._datetime_sub_dirs(timestamp)
            )

    def compressed_file_path(self, timestamp, feed_id):
        timestamp = timestamp - timestamp % 3600
        return os.path.join(
            self.compressed_hour_dir(timestamp),
            self._file_name(timestamp, feed_id, 'tar.bz2')
            )

    # (1.5) Archive task

    def archived_hour_key(self, timestamp):
        timestamp = timestamp - timestamp % 3600
        return self._datetime_sub_dirs(timestamp)

    def archived_file_key(self, timestamp, feed_id):
        timestamp = timestamp - timestamp % 3600
        return os.path.join(
            self.archived_hour_key(timestamp),
            self._file_name(timestamp, feed_id, 'tar.bz2')
            )

    # (2) PART TWO: Functions to list certain types of file on disk

    # (2.0) Utility functions

    def _interpret_file_name(self, file_name, expected_affix=''):
        """Interpret a file name as a feed download."""
        try:
            i = file_name.find('-')
            j = file_name.rfind('.')
            feed_id = file_name[:i]
            iso_8601 = file_name[i+1:j-len(expected_affix)]
            affix = file_name[j-len(expected_affix):j]
            feed_ext = file_name[j+1:]

            if affix != expected_affix:
                return (None, None)

            if feed_id not in self.feeds_by_id:
                return (None, None)
            feed = self.feeds_by_id[feed_id]

            timestamp = _iso_8601_to_timestamp(iso_8601)

        except Exception:
            return (None, None)

        return (feed, timestamp)

    def _list_feeds_in_dir(self, dir_path, current_time, affix=''):
        for sub_dir, _, file_names in os.walk(dir_path):
            for file_name in file_names:
                file_path = os.path.join(sub_dir, file_name)
                file_time = os.path.getmtime(file_path)
                if(current_time - file_time < self._file_access_lag):
                    continue
                (feed, timestamp) = self._interpret_file_name(file_name, affix)
                if feed is None:
                    print(file_name)
                    continue

                yield (feed, timestamp, file_path)

    # (2.1) Download task

    def list_downloaded_feeds(self, current_time):
        yield from self._list_feeds_in_dir(
            self.downloaded_root_dir,
            current_time,
            '-dt'
            )

    # (2.2) Filter task

    def list_filtered_feeds(self, current_time):
        yield from self._list_feeds_in_dir(
            self.filtered_root_dir,
            current_time,
            )

    def list_filtered_hours(self, current_time, list_all=False):
        candidate_dirs = glob.glob(
            os.path.join(
                self.filtered_root_dir,
                '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/[0-9][0-9]'
                )
            )
        for candidate_dir in candidate_dirs:
            j = candidate_dir.rfind('/')
            i = candidate_dir.rfind('/', 0, j)
            date = candidate_dir[i+1:j]
            hour = candidate_dir[j+1:]
            iso_8601 = '{}T{}0000Z'.format(date, hour)
            timestamp = _iso_8601_to_timestamp(iso_8601)
            cond_1 = not list_all
            cond_2 = current_time - timestamp < self._file_access_lag
            if cond_1 and cond_2:
                continue
            yield (candidate_dir, timestamp)

    def list_feeds_in_filtered_hour(self, feeds, hour_dir):
        for feed in feeds:
            feed_id = feed[0]
            feed_dir = os.path.join(hour_dir, feed_id)
            if not os.path.isdir(feed_dir):
                yield (feed, None)
            else:
                yield (feed, feed_dir)

    # (2.3) Compress task

    def list_compressed_hours(self, current_time):
        feeds = self.feeds
        for sub_dir, _, file_names in os.walk(self.compressed_root_dir):
            for file_name in file_names:
                if file_name[-7:] != 'tar.bz2':
                    continue
                modified_time = os.path.getmtime(
                    os.path.join(sub_dir, file_name)
                    )
                if(current_time - modified_time < self._file_access_lag):
                    continue
                i = file_name.find('-')
                feed_id = file_name[:i]
                if feed_id not in self.feeds_by_id:
                    continue
                iso_8601 = file_name[i+1:-8]
                yield (
                    self.feeds_by_id[feed_id],
                    _iso_8601_to_timestamp(iso_8601),
                    os.path.join(sub_dir, file_name)
                    )


class FileSystem():
    """Provides basic file system interaction methods."""

    def __init__(self):
        pass

    def isfile(self, file_path):
        """Check if a file exists.

        The argument is passed straight to os.path.isfile().
        """
        return os.path.isfile(file_path)

    def copyfile(self, source_file_path, target_file_path):
        """Copy a file.

        The directory in the target path will be created automatically if
        it does not exists. The arguments are then passed to shutil.copyfile().
        """
        self.ensure_dir(os.path.dirname(target_file_path))
        shutil.copyfile(source_file_path, target_file_path)

    def remove(self, file_path):
        """Remove a file.

        The argument is passed straight to os.remove().
        """
        os.remove(file_path)

    def num_files_in_dir(self, dir_path):
        """Return the number of files located in a directory."""
        return len([
            name for name
            in os.listdir(dir_path)
            if os.path.isfile(os.path.join(dir_path, name))
            ])

    def content(self, file_path):
        """Return the content of a file.

        The file is opened in binary format.
        """
        with open(file_path, 'rb') as f:
            s = f.read()
        return s

    def ensure_dir(self, dir_path):
        """If a directory does not exist, make it."""
        try:
            os.makedirs(dir_path)
        except FileExistsError:
            pass

    def rmtree(self, dir_path):
        """Remove a directory and everything containined inside it.

        The argument is passed to shutil.rmtree(); however, if the directory
        does not exist the exception from shutil.rmtree() is suppressed.
        """
        try:
            shutil.rmtree(dir_path)
        except FileNotFoundError:
            pass

    def write_to_file(self, s, file_path):
        """Write data to a file.

        The directory continaing the file need not exists."""
        # If the directory into which the file will be written doesn't exists,
        # create it.
        try:
            os.makedirs(os.path.dirname(file_path))
        except FileExistsError:
            pass

        # Then write in the data.
        with open(file_path, 'wb') as f:
            f.write(s)

    def dir_to_tar_file(
            self, directory, tar_file, overwrite=False, remove_directory=True
            ):
        """Compress the contents of the given directory into a tar archive.

        The contents of the directory and the directory itself will be deleted
        after compression if remove_directory is True.
        Unless overwrite is True, a FileExistsError
        exception will be thrown if tar_file already exists.
        """

        # The exception throwing is determined simply by the file opening mode.
        if overwrite:
            s = 'w'
        else:
            s = 'x'

        # Perform the operation.
        self.ensure_dir(os.path.dirname(tar_file))
        tar_handle = tarfile.open(tar_file, s+':bz2')
        tar_handle.add(directory, arcname='')
        tar_handle.close()
        if remove_directory:
            self.rmtree(directory)

    def tar_file_to_dir(self, tar_file, directory, remove_tar_file=True):
        """Extract the given tar archive into a directory.

        If remove_tar_file is true, the tar file will be deleted after
        unpacking.
        """
        self.ensure_dir(directory)
        tar_handle = tarfile.open(tar_file, 'r:bz2')
        tar_handle.extractall(directory)
        tar_handle.close()
        if remove_tar_file:
            self.remove(tar_file)

    def touch(self, file_path):
        """Create an empty file if it does not exist."""
        try:
            open(file_path, 'x')
        except FileExistsError:
            pass

    def prune_dir_tree(self, dir_path, delete_self=False):
        """Remove all sub directories that do not contain files in their trees.

        If delete_self is True, also delete the root directory if the
        whole tree contains no files.
        """

        if not os.path.isdir(dir_path):
            return 0
        # The pruning occurs by a depth first search, implemented
        # through recursive calls to this function.

        # The variable contains_files will tell whether the current directory
        # contains any files or directories with files in their trees.
        contains_files = False
        # total counts the number of directory deletions.
        total = 0
        # Iterate through each child of the current node.
        for entry in os.listdir(dir_path):
            path = os.path.join(dir_path, entry)
            # If the child is a directory, traverse down it recursively.
            # The delete_self flag is true, meaning the child itself will be
            # deleted if there are no files in its directory tree.
            if os.path.isdir(path):
                total += self.prune_dir_tree(path + '/', True)
                if os.path.isdir(path + '/'):
                    contains_files = True
            # If this child is not a directory, the node will not be deleted.
            else:
                if entry[0] == '.':
                    os.remove(path)
                else:
                    contains_files = True
        # Based on the result, delete the present node or not.
        if contains_files is False and delete_self is True:
            os.rmdir(dir_path)
            total += 1
        return total

    def md5_hash(self, file_path):
        """Calculate the MD5 hash of a file."""
        with open(file_path, mode='rb') as f:
            d = hashlib.md5()
            for buf in iter(functools.partial(f.read, 128), b''):
                d.update(buf)
        return d.hexdigest()
