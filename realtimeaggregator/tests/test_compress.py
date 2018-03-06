import time
import os
import requests.exceptions
import shutil
from . import utils
from . import virtualobjects
from .. import tasks
from ..logs import log_templates_pb2


# The testing process for all tasks has a similar 1-7 process; see the
# testing README.md for information on this.
def test_compress(quiet=True):

    # (1) Initialize the download task.
    print('Running virtual compress task.')
    task_init_args = {
            'storage_dir': 'unittests',
            'feeds': [],
            'quiet': quiet,
        }
    task = tasks.compress.CompressTask(**task_init_args)
    task.limit = -1
    task.file_access_lag = 0
    task.compress_all = True
    task._log_file_path = os.path.join('unittests', 'compress.log')

    # (2) Refresh the directory to be used for testing.
    file_system = task._file_system
    file_system.rmtree('unittests')

    # (3) Initialize the virtual objects.
    # Initialize the virtual downloader with 3 feeds.
    # The clock increment is greater than all refresh_periods;
    # this ensures no duplicates. (Not that it really matters.)
    vdownloader = virtualobjects.VirtualDownloader(
        clock_init=(time.time() - (time.time() % 3600) - 2000),
        clock_increment=40
        )
    vdownloader.set_bad_download_ratios(0, 0, 0.25)
    vdownloader.add_virtual_feed(refresh_period=30)
    vdownloader.add_virtual_feed(refresh_period=20)
    vdownloader.add_virtual_feed(refresh_period=15)
    vdownloader.attach_to(task)

    # (4) Write initial files to the file system.
    # First, we need to have filtered files to compress.
    # To preserve unit test independence these files will be created
    # without the FilterTask object.
    # Copy feeds to filtered directory
    for _ in range(100):
        for (feed_id, feed_url, feed_ext, _) in vdownloader.feeds:
            target_file_path = task._files_schema.filtered_file_path(
                vdownloader.time(),
                feed_id,
                feed_ext
                )
            try:
                vdownloader.download(
                    feed_url,
                    target_file_path,
                    file_system
                    )
            except requests.exceptions.RequestException:
                pass

    # (5) Perform the task.
    try:
        task.run()
    except Exception as e:
        print('Error: encountered exception in unit test run.')
        raise e

    # (6) Collect post-task data.
    # hours_to_check will be a list of all hours for which either the log
    # or the files system reports activity
    hours_to_check = {}

    # (6.1) collect stored data from the file system.
    files_data = {}
    iterator = task._files_schema.list_compressed_hours(task.time())
    for ((_, feed_url, _, _), timestamp, file_path) in iterator:
        # Initialize some of the data variables for this feed.
        if feed_url not in files_data:
            hours_to_check[feed_url] = set()
            files_data[feed_url] = {}
        hours_to_check[feed_url].add(timestamp)
        # Unpack the tar file and count how many files are in it.
        # This should be abstracted away with a num_files_in_tar_file
        # method of the file system.
        tmp_dir = os.path.join('unittests', 'tmp')
        file_system.tar_file_to_dir(
            file_path,
            tmp_dir,
            remove_tar_file=False
            )
        files_data[feed_url][timestamp] = (
            task._file_system.num_files_in_dir(tmp_dir)
            )
        file_system.rmtree(tmp_dir)

    # (6.2) collect information from the log file.
    log = log_templates_pb2.CompressTaskLog()
    log_data = {}
    feed_url_by_feed_index = {}
    with open(task._log_file_path, 'rb') as f:
        log.ParseFromString(f.read())
    for compressed_hour in log.compressed_hours:
        for i in range(len(log.feeds)):
            feed_url = log.feeds[i].feed_url
            if feed_url not in log_data:
                log_data[feed_url] = {}
            log_data[feed_url][compressed_hour.timestamp] = (
                compressed_hour.num_compressed[i]
                )
            hours_to_check[feed_url].add(compressed_hour.timestamp)

    # (7) Perform the comparisons.

    # Iterate over each feed
    row = '  {: <12} | {: <12} | {: <12} | {: <12}'
    for feed_url, server_data in vdownloader.responses.items():
        # Print table heading
        print('  ----')
        print('  Virtual feed: {}'.format(feed_url))
        print(row.format('hour', 'v server', 'files', 'log file'))

        # TEST 1
        # Iterate over each hour and ensure that the data about that hour
        # in the log matches the data from the file system.
        t1 = True
        for hour in hours_to_check[feed_url]:
            t1 = t1 and utils._compare(
                row,
                hour,
                None,
                files_data[feed_url][hour],
                log_data[feed_url][hour]
                )

        # TEST 2
        # Compare the total number of feeds (summed over all hours) between
        # the virtual server, file system and log.
        t2 = utils._compare(
            row,
            'total',
            server_data['valid'],
            sum(files_data[feed_url].values()),
            sum(log_data[feed_url].values())
        )

        utils._pass_or_fail(t1 and t2)
