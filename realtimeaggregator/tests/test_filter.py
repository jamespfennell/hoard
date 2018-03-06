import os
import time
from . import utils
from . import virtualobjects
from .. import tasks
from ..logs import log_templates_pb2


# The testing process for all tasks has a similar 1-7 process; see the
# testing README.md for information on this.
def test_filter(quiet=True):
    """Test the filter task."""

    # (1) Initialize the filter task.
    print('Running virtual filter task.')
    task_init_args = {
            'storage_dir': 'unittests',
            'feeds': [],
            'quiet': quiet,
        }
    task = tasks.filter.FilterTask(**task_init_args)
    task.limit = -1
    task.file_access_lag = 0
    task._log_file_path = os.path.join('unittests', 'filter-.log')

    # (2) Refresh the directory to be used for testing.
    file_system = task._file_system
    file_system.rmtree('unittests')

    # (3) Initialize the virtual objects.
    # For the download task we use a virtual downloader
    # with 3 feeds.
    vdownloader = virtualobjects.VirtualDownloader(
        clock_init=time.time(),
        clock_increment=1
        )
    vdownloader.set_bad_download_ratios(0.25, 0.25, 0)
    vdownloader.add_virtual_feed(refresh_period=30)
    vdownloader.add_virtual_feed(refresh_period=20)
    vdownloader.add_virtual_feed(refresh_period=15)
    vdownloader.attach_to(task)

    # (4) Write initial files to the file system.
    # We need to have downloaded files to filter.
    # To preserve unit test independence these files will be created
    # without the DownloadTask object.

    # Attempt to download each feed 200 times.
    for _ in range(200):
        for (feed_id, feed_url, feed_ext, _) in vdownloader.feeds:
            source_file_path = task._files_schema.downloaded_file_path(
                vdownloader.time(),
                feed_id,
                feed_ext
                )
            vdownloader.download(
                feed_url,
                source_file_path,
                file_system
                )

    # (5) Perform the task.
    try:
        task.run()
    except Exception as e:
        print('Error: encountered exception in unit test run.')
        raise e

    # (6) Collect post-task data.

    # (6.1) collect stored data from the file system.
    files_data = utils._analyze_unittest_feeds(
        task._files_schema.list_filtered_feeds(task.time())
        )

    # (6.2) collect information from the log file.
    log = log_templates_pb2.FilterTaskLog()
    with open(task._log_file_path, 'rb') as f:
        log.ParseFromString(f.read())
    log_data = {}
    for i in range(len(log.feeds)):
        feed_url = log.feeds[i].feed_url
        log_data[feed_url] = {
            'copied': log.num_copied[i],
            'duplicate': log.num_duplicate[i],
            'corrupt': log.num_corrupt[i]
            }

    # (7) Perform the comparisons.

    # Iterate over each feed
    row = '  {: <12} | {: <12} | {: <12} | {: <12}'
    for feed_url, server_data in vdownloader.responses.items():
        # Print the Table heading
        print('  ----')
        print('  Virtual feed: {}'.format(feed_url))
        print(row.format('type', 'v server', 'files', 'log file'))

        # TEST 1
        # Compare the number of valid feed downloads.
        t1 = utils._compare(
            row,
            'unique',
            server_data['unique'],
            files_data[feed_url]['valid'],
            log_data[feed_url]['copied'],
        )

        # TEST 2
        # Compare the numbr of duplicate feed downloads.
        # The file system doesn't see this as it doens't compare feeds
        # with feeds.
        t2 = utils._compare(
            row,
            'duplicate',
            server_data['valid'] - server_data['unique'],
            None,
            log_data[feed_url]['duplicate']
            )

        # TEST 3
        # Compare the number of corrupt feeds.
        # The file system doesn't see this as all the corrupt feeds have been
        # deleted.
        t3 = utils._compare(
            row,
            'corrupt',
            server_data['corrupt'] + server_data['empty'],
            None,
            log_data[feed_url]['corrupt']
            )

        # Print the pass/fail message.
        utils._pass_or_fail(t1 and t2 and t3)
