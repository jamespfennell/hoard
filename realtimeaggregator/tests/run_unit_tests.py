    
import time
import random
import string
import requests.exceptions
import tasks
import shutil
import os

from tasks import tools

from tasks.common import log_templates_pb2

def unittest_feed_reader(file_path):
    """Check if a unittest feed is valid and, if so, return its timestamp.
    
    To be implemented!
    """
    with open(file_path, 'r') as f:
        content = f.read()
    return int(content)

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
        The proportion of regular feeds will be 1 minues the sum
        of the arguments.
        """
        assert corrupt >=0 and empty >= 0 and failed >= 0
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
        feed_func = unittest_feed_reader;
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
            'unique' : 0,
            }

        return feed_url

    def time(self):
        """Return the current time of the internal clock."""
        return self.clock
    
    def download(self, url, file_path):
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
                /self.refresh_periods[url]
                )
            # Then add that much time to the last refresh time
            if num_refresh > 0:
                self.last_refresh[url] += num_refresh * self.refresh_periods[url]
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

        # Write the virtual feed to disk
        with open(file_path, 'wb') as f:
            f.write(response.encode('utf-8'))
        
    def attach_to(self, task):
        """Replace the downloader in a task with this virtual downloader.
        
        As well as replacing the downloader object, this function also
        imports the virtual feeds and it has the task read time from
        this virtual server.
        """
        task.feeds = self.feeds
        task._downloader = self
        task.time = self.time


def _erase_unittests_dir():
    """Erase all of the daya in the unittests directory."""
    try:
        shutil.rmtree('unittests')
    except FileNotFoundError:
        pass


def _list_unittest_feeds(dir_path):

    # First, collect information about the downloaded files on disk.
    files_by_feed_url = {}
    for directory, _, files in os.walk(dir_path):
        for file_name in files:
            if file_name[0] == '.':
                continue
            if file_name[file_name.rfind('.'):] != '.unittest':
                continue
            # Infer the feed url from the file name
            feed_url = 'unittest://{}.unittest'.format(
                file_name[:file_name.find('-')]
                )
            # If this is the first time seeing this feed,
            # initialize the downloaded files count
            if feed_url not in files_by_feed_url:
                files_by_feed_url[feed_url] = {
                    'valid': 0,
                    'corrupt': 0,
                    'empty': 0
                    }
    
            # Read the content to see what kind of feed download it is.
            # We want to migrate to using the protocal buffer Unittest
            # feed type, but for the moment we do things more basicallly.
            with open(os.path.join(directory, file_name), 'r') as f:
                content = f.read()
            if content == '':
                files_by_feed_url[feed_url]['empty'] += 1
            else:
                try:
                    int(content)
                    files_by_feed_url[feed_url]['valid'] += 1
                except ValueError:
                    files_by_feed_url[feed_url]['corrupt'] += 1
    return files_by_feed_url






def test_download_task():
    """Perform a unit test of the download task."""

    _erase_unittests_dir()

    # Initialize the virtual downloader with 3 feeds.
    vdownloader = VirtualDownloader(clock_increment=5)
    vdownloader.set_bad_download_ratios(0.25, 0.25, 0.25)
    vdownloader.add_virtual_feed(refresh_period=30)
    vdownloader.add_virtual_feed(refresh_period=20)
    vdownloader.add_virtual_feed(refresh_period=15)

    # Perform the task.
    task_init_args = {
            'storage_dir': 'unittests',
            'feeds': [],
            'quiet': True,
        }

    task = tasks.download.DownloadTask(**task_init_args)
    vdownloader.attach_to(task)
    task.frequency = 5.00
    task.duration = 200
    task._log_file_path = 'unittests/download.log'
    try:
        with task:
            print('Running virtual download task.')
            task.run()
    except Exception as e:
        print('Error: encountered exception in unit test run.')
        raise e


    # Now that the task is completed, we check it worked.
    # We are going to cross check the data given by the virtual server
    # versus the files we find on disk versus what the log says.
    
    # First, collect information about the downloaded files on disk.
    downloaded_files_by_feed_url = _list_unittest_feeds('unittests/feeds')

    # Second, collect information from the log file
    log = log_templates_pb2.DownloadTaskLog()
    with open('unittests/download.log', 'rb') as f:
        log.ParseFromString(f.read())
    logged_files_by_feed_url = {}
    for i in range(len(log.num_downloaded)):
        feed_url = log.feeds[i].feed_url
        logged_files_by_feed_url[feed_url] = log.num_downloaded[i]

    # Now peform the comparison
    print('Comparing:')
    print('  * virtual server responses')
    print('  * downloaded files on disk')
    print('  * log file contents')
    
    # Iterate over each feed
    row = '  {: <15} | {: <12} | {: <12} | {: <12}'
    for feed_url, server_counts in vdownloader.responses.items():
        passed = True
        # Table heading
        print('  ----')
        print('  Virtual feed: {}'.format(feed_url))
        print(row.format('type','server','downloads', 'log file'))

        # First compare the feed information in the virtual server versus
        # the files on disk. In this case we can make seperate tests
        # for each of the three types of downloaded feed: valid, empty
        # or corrupt.
        downloaded_total = 0
        for feed_type in ['valid', 'corrupt', 'empty']:
            b = downloaded_files_by_feed_url[feed_url][feed_type]
            downloaded_total += b
            t1 = _compare(
                row,
                feed_type,
                server_counts[feed_type],
                b,
                None
                )
            passed = passed and t1

        # Next compare the total downloads in the server vs log vs 
        # downloaded files.
        t2 = _compare(
            row,
            'total', 
            server_counts['valid'] + server_counts['corrupt'] + server_counts['empty'],
            downloaded_files_by_feed_url[feed_url]['valid']
            + downloaded_files_by_feed_url[feed_url]['corrupt']
            + downloaded_files_by_feed_url[feed_url]['empty'],
            logged_files_by_feed_url[feed_url]
            )

        # Finally compare the number of failed downloads in the log versus
        # the virtual server.
        t3 = _compare(
            row,
            'failed download',
            server_counts['failed'],
            None,
            log.num_cycles-logged_files_by_feed_url[feed_url]
            )

        # Print message.
        if passed and t2 and t3:
            print('\033[92m\033[01m  Passed \033[0m')
        else:
            print('\033[91m\033[01m  Failed \033[0m')




def test_filter_task():

    _erase_unittests_dir()
    print('Testing filter task.')

    # First, we need to have downloaded files to filter.
    # To preserve unit test independence these files will be created
    # without the DownloadTask object.

    # Initialize the virtual downloader with 3 feeds.
    vdownloader = VirtualDownloader(
        clock_init = time.time(),
        clock_increment=1
        )
    vdownloader.set_bad_download_ratios(0.25, 0.25, 0)
    vdownloader.add_virtual_feed(refresh_period=30)
    vdownloader.add_virtual_feed(refresh_period=20)
    vdownloader.add_virtual_feed(refresh_period=15)

    # Prepare the downloadeded dictory
    source_dir = 'unittests/feeds/downloaded'
    tools.filesys.ensure_dir(source_dir)
    
    # Copy feeds to downloaded directory
    for _ in range(200):
        for (feed_id, feed_url, feed_ext, _) in vdownloader.feeds:
            source_file_name = '{}-{}-dt.{}'.format(
                feed_id,
                tools.time.timestamp_to_utc_8601(vdownloader.time()),
                feed_ext
                )
            vdownloader.download(
                feed_url,
                os.path.join(source_dir, source_file_name)
                )


    # Now perform the filter task
    task_init_args = {
            'storage_dir': 'unittests',
            'feeds': [],
            'quiet': True,
        }
    task = tasks.filter.FilterTask(**task_init_args)
    vdownloader.attach_to(task)
    task.limit = -1
    task.file_access_lag = 0
    task._log_file_path = 'unittests/filter.log'
    print('Running virtual filter task.')
    try:
        with task:
            task.run()
    except Exception as e:
        print('Error: encountered exception in unit test run.')
        raise e



    filtered_feeds_by_feed_url = _list_unittest_feeds('unittests/feeds')

    # Second, collect information from the log file
    log = log_templates_pb2.FilterTaskLog()
    with open('unittests/filter.log', 'rb') as f:
        log.ParseFromString(f.read())

    logged_feeds_by_feed_url = {}

    for i in range(len(log.feeds)):
        feed_url = log.feeds[i].feed_url
        logged_feeds_by_feed_url[feed_url] = {
            'copied': log.num_copied[i],
            'duplicate': log.num_duplicate[i],
            'corrupt': log.num_corrupt[i]
            }

    # Iterate over each feed
    row = '  {: <12} | {: <12} | {: <12} | {: <12}'
    for feed_url, server_counts in vdownloader.responses.items():
        passed = True
        # Table heading
        print('  ----')
        print('  Virtual feed: {}'.format(feed_url))
        print(row.format('type','v server','files', 'log file'))

        # First compare the feed information in the virtual server versus
        # the files in the log. In this case we can make seperate tests
        # for each of the three types of downloaded feed: valid, corrupt,
        # duplicate
        t1 = _compare(
            row,
            'unique',
            server_counts['unique'],
            filtered_feeds_by_feed_url[feed_url]['valid'],
            logged_feeds_by_feed_url[feed_url]['copied'],
        )

        t2 = _compare(
            row,
            'duplicate',
            server_counts['valid'] - server_counts['unique'],
            None,
            logged_feeds_by_feed_url[feed_url]['duplicate']
            )

        t3 = _compare(
            row,
            'corrupt',
            server_counts['corrupt'] + server_counts['empty'],
            None,
            logged_feeds_by_feed_url[feed_url]['corrupt']
            )

        # Print message.
        if t1 and t2 and t3:
            print('  {}'.format(_green('Passed')))
        else:
            print('  {}'.format(_red('Failed')))



def test_compress_task():

    _erase_unittests_dir()
    print('Testing compress task.')

    # First, we need to have filtered files to compress.
    # To preserve unit test independence these files will be created
    # without the FilterTask object.

    # Initialize the virtual downloader with 3 feeds.
    # The clock increment is greater than all refresh_periods;
    # this ensures no duplicates. (Not that it really matters.)
    vdownloader = VirtualDownloader(
        clock_init = time.time() - time.time()%3600 - 2000,
        clock_increment=40
        )
    vdownloader.set_bad_download_ratios(0, 0, 0)
    vdownloader.add_virtual_feed(refresh_period=30)
    vdownloader.add_virtual_feed(refresh_period=20)
    vdownloader.add_virtual_feed(refresh_period=15)

    # Prepare the downloadeded dictory
    source_root_dir = 'unittests/feeds/filtered'
    
    # Copy feeds to filtered directory
    for i in range(100):
        for (feed_id, feed_url, feed_ext, _) in vdownloader.feeds:
            if feed_id == 'feed1' and i%4 == 0:
                continue
            if feed_id == 'feed2' and i%3 == 0:
                continue
            (day_dir, hour_dir, file_time)= (
                tools.time.timestamp_to_path_pieces(vdownloader.time())
                )
            source_dir = os.path.join(
                source_root_dir,
                day_dir,
                hour_dir,
                feed_id
                )
            tools.filesys.ensure_dir(source_dir)
            source_file_name = '{}-{}-dt.{}'.format(
                feed_id,
                tools.time.timestamp_to_utc_8601(vdownloader.time()),
                feed_ext
                )
            vdownloader.download(
                feed_url,
                os.path.join(source_dir, source_file_name)
                )

    # Now perform the compress task
    task_init_args = {
            'storage_dir': 'unittests',
            'feeds': [],
            'quiet': True,
        }
    task = tasks.compress.CompressTask(**task_init_args)
    vdownloader.attach_to(task)
    task.limit = -1
    task.file_access_lag = 0
    task.compress_all = True
    task._log_file_path = 'unittests/compress.log'
    print('Running virtual compress task.')
    try:
        with task:
            task.run()
    except Exception as e:
        print('Error: encountered exception in unit test run.')
        raise e


    # Read in the log
    
    # Second, collect information from the log file
    log = log_templates_pb2.CompressTaskLog()
    logged_tarbz2_files_by_feed_url = {}
    feed_url_by_feed_index = {}
    logged_num_compressed_by_feed_url = {}
    with open('unittests/compress.log', 'rb') as f:
        log.ParseFromString(f.read())

    logged_feeds_by_feed_url = {}

    for i in range(len(log.feeds)):
        feed_url = log.feeds[i].feed_url
        feed_url_by_feed_index[i] = feed_url
        logged_num_compressed_by_feed_url[feed_url] = 0
        logged_tarbz2_files_by_feed_url[feed_url] = set()

    for compressed_hour in log.compressed_hours:
        for i in range(len(log.feeds)):
            logged_num_compressed_by_feed_url[
                feed_url_by_feed_index[i]
                ] += compressed_hour.num_compressed[i]
            logged_tarbz2_files_by_feed_url[
                feed_url_by_feed_index[i]
                ].add(
                os.path.join(
                    compressed_hour.target_directory,
                    compressed_hour.target_file_names[i]
                )
                )


    actual_num_compressed_by_feed_url = {}
    for feed_url, tarbz2_files in logged_tarbz2_files_by_feed_url.items():
        tools.filesys.ensure_dir('unittests/tmp')
        for tarbz2_file in tarbz2_files:
            tools.filesys.tar_file_to_directory(
                tarbz2_file,
                'unittests/tmp',
                remove_tar_file = False
                )

        actual_num_compressed_by_feed_url[feed_url] = (
                len([name for name in os.listdir('unittests/tmp')])
            )
        try:
            shutil.rmtree('unittests/tmp')
        except FileNotFoundError:
            pass

    # Iterate over each feed
    row = '  {: <12} | {: <12} | {: <12} | {: <12}'
    for feed_url, server_counts in vdownloader.responses.items():
        passed = True
        # Table heading
        print('  ----')
        print('  Virtual feed: {}'.format(feed_url))
        print(row.format('type','v server','files', 'log file'))

        # First compare the feed information in the virtual server versus
        # the files in the log. In this case we can make seperate tests
        # for each of the three types of downloaded feed: valid, corrupt,
        # duplicate
        t1 = _compare(
            row,
            'feeds',
            server_counts['valid'],
            actual_num_compressed_by_feed_url[feed_url],
            logged_num_compressed_by_feed_url[feed_url],
        )
        # Print message.
        if t1:
            print('  {}'.format(_green('Passed')))
        else:
            print('  {}'.format(_red('Failed')))


class VirtualTransferer():

    def __init__(self, storage_dir, error_rate=0):
        assert error_rate >=0 and error_rate <=1
        self._storage_dir = storage_dir
        tools.filesys.ensure_dir(storage_dir)
        self.error_rate = error_rate
        self.num_download_errors = 0
        self.num_upload_errors = 0
        self.objects_handled = set()

    def _location(self, object_key):
        return os.path.join(
            self._storage_dir,
            'id{}.object'.format(hash(object_key))
            )


    def upload(self, file_path, object_key, remove_original=False):
        self.objects_handled.add(self._location(object_key))
        if random.random() < self.error_rate:
            self.num_upload_errors += 1
            raise Exception('Simulated storage transferer upload error.')
        if remove_original:
            operation = os.rename
        else:
            operation = shutil.copyfile
        operation(file_path, self._location(object_key))


    def download(self, object_key, file_path):
        self.objects_handled.add(self._location(object_key))
        if random.random() < self.error_rate:
            self.num_download_errors += 1
            raise Exception('Simulated storage transferer download error.')
        os.rename(self._location(object_key), file_path)

    def md5_hash(self, object_key):
        self.objects_handled.add(self._location(object_key))
        try:
            return tools.filesys.md5_hash(self._location(object_key))
        except FileNotFoundError:
            raise self.ObjectDoesNotExist

    def attach_to(self, task):
        task._transferer = self

    class ObjectDoesNotExist(Exception):
        pass

def test_archive_task():


    _erase_unittests_dir()
    print('Testing archive task.')

    # First, we need to have filtered files to compress.
    # To preserve unit test independence these files will be created
    # without the FilterTask object.

    # Initialize the virtual downloader with 3 feeds.
    # The clock increment is greater than all refresh_periods;
    # this ensures no duplicates. (Not that it really matters.)
    vdownloader = VirtualDownloader(
        clock_init = time.time() - time.time()%3600 - 2000,
        clock_increment=40
        )
    vdownloader.set_bad_download_ratios(0, 0, 0.25)
    for _ in range(15):
        vdownloader.add_virtual_feed(refresh_period=25)

    

    tmp_dir = os.path.join('unittests', 'tmp')
    bucket_dir = os.path.join(tmp_dir, 'bucket')
    vtransferer = VirtualTransferer(bucket_dir)

    (day_dir, hour_dir, file_time) = tools.time.timestamp_to_path_pieces()
    compressed_dir = os.path.join('unittests', 'feeds', 'compressed')
    target_dir = os.path.join(compressed_dir, day_dir, hour_dir)
    tools.filesys.ensure_dir(target_dir)


    first_run = True
    i = 0;
    for _ in range(2):
        for (feed_id, feed_url, _, _) in vdownloader.feeds:
            if first_run and random.random()<= 0.5:
                continue

            source_dir = os.path.join(tmp_dir, feed_id)
            tools.filesys.ensure_dir(source_dir)

            for k in range(30):
                try:
                    vdownloader.download(
                        feed_url,
                        os.path.join(source_dir, '{}-{}{}'.format(feed_id, k,first_run))
                        )
                except requests.exceptions.RequestException:
                    pass
                    

            target_file_name = '{}-{}T{}.tar.bz2'.format(
                        feed_id,
                        day_dir,
                        hour_dir
                        )
            target_file_path = os.path.join(target_dir, target_file_name)
            target_key = os.path.join(day_dir, hour_dir, target_file_name)
                    
            tools.filesys.directory_to_tar_file(source_dir, target_file_path)

            if first_run:
                vtransferer.upload(target_file_path, target_key, remove_original=True)

            i += 1;
        first_run = False

    # Now perform the archivs task
    task_init_args = {
            'storage_dir': 'unittests',
            'feeds': [],
            'quiet': True,
        }
    task = tasks.archive.ArchiveTask(**task_init_args)
    vdownloader.attach_to(task)
    vtransferer.objects_handled = set()
    vtransferer.error_rate = 0.5
    vtransferer.attach_to(task)
    task.limit = -1
    task.file_access_lag = 0
    task._log_file_path = 'unittests/archive.log'
    print('Running virtual archive task.')
    try:
        with task:
            task.run()
    except Exception as e:
        print('Error: encountered exception in unit test run.')
        raise e
    


    # Second, collect information from the log file
    log = log_templates_pb2.ArchiveTaskLog()
    status_by_feed_id = {}
    with open('unittests/archive.log', 'rb') as f:
        log.ParseFromString(f.read())

    logged_feeds_by_feed_url = {}

    logged_num_success = 0
    logged_num_download_error = 0
    logged_num_upload_error = 0
    for i in range(len(log.uploads)):
        feed_id = log.uploads[i].feed_id
        if log.uploads[i].success:
            status_by_feed_id[feed_id] = 'success'
            logged_num_success += 1
        elif log.uploads[i].download_error.name != '':
            status_by_feed_id[feed_id] = 'download_error'
            logged_num_download_error += 1
        else:
            status_by_feed_id[feed_id] = 'upload_error'
            logged_num_upload_error += 1


    row = '  {: <19} | {: <12} | {: <12} | {: <12}'
    print('  ----')
    print(row.format('type','v server','v transferer', 'log file'))
    t1 = _compare(row, 'num upload attempts', 
        len(vdownloader.feeds), 
        len(vtransferer.objects_handled),
        len(log.uploads)
        )
    t2 = _compare(row, 'download errors', None, vtransferer.num_download_errors,
        logged_num_download_error)
    t3 =_compare(row, 'upload errors', None, vtransferer.num_upload_errors,
        logged_num_upload_error)
    if t1 and t2 and t3:
        print('  {}'.format(_green('Passed')))
    else:
        print('  {}'.format(_red('Failed')))


    print('  For each succesful feed, will now verify that feeds were stored correctly')

    print('  ----')
    print(row.format('feed id','v server','on disk', ''))
    vtransferer.error_rate = 0


    passed = True

    for (feed_id, feed_url, _, _) in vdownloader.feeds:
        if status_by_feed_id[feed_id] != 'success':
            continue
        target_file_name = '{}-{}T{}.tar.bz2'.format(
                    feed_id,
                    day_dir,
                    hour_dir
                    )
        source_key = os.path.join(day_dir, hour_dir, target_file_name)
        target_dir = os.path.join('unittests', 'tmp', feed_id)
        tools.filesys.ensure_dir(target_dir)
        target_file_path = os.path.join(target_dir, target_file_name)
        vtransferer.download(source_key, target_file_path)
        tools.filesys.tar_file_to_directory(target_file_path, target_dir)
        
        t1 = _compare(row, feed_id,
            len([name for name in os.listdir(target_dir)]),
            vdownloader.responses[feed_url]['valid'],
            None
            )

        passed = passed and t1
        
    if passed:
        print('  {}'.format(_green('Passed')))
    else:
        print('  {}'.format(_red('Failed')))

def _green(s):
    return '\033[92m\033[01m{}\033[0m'.format(s)

def _red(s):
    return '\033[91m\033[01m{}\033[0m'.format(s)


def _compare(row, field, a, b, c):
    if a is None:
        a = ''
        test = (b == c)
    elif b is None:
        b = ''
        test = (a == c)
    elif c is None:
        c = ''
        test = (a == b)
    else:
        test = (a == b) and (b == c)
    print(row.format(field, a, b, c))
    return test


def print_download_task_log(file_path):
    l = log_templates_pb2.DownloadTaskLog()
    with open(file_path, 'rb') as f:
        l.ParseFromString(f.read())
    print(l)

def print_filter_task_log(file_path):
    l = log_templates_pb2.FilterTaskLog()
    with open(file_path, 'rb') as f:
        l.ParseFromString(f.read())
    print(l)

def print_compress_task_log(file_path):
    l = log_templates_pb2.CompressTaskLog()
    with open(file_path, 'rb') as f:
        l.ParseFromString(f.read())
    print(l)

def print_archive_task_log(file_path):
    l = log_templates_pb2.ArchiveTaskLog()
    with open(file_path, 'rb') as f:
        l.ParseFromString(f.read())
    print(l)



#test_download_task()
#test_filter_task()
#test_compress_task()
#
test_archive_task()

#print('LOG:')
#print_compress_task_log('unittests/compress.log')
#
#print_archive_task_log('unittests/archive.log')





