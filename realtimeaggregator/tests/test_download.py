import os
from . import utils
from . import virtualobjects
from .. import tasks
from ..logs import log_templates_pb2


# The testing process for all tasks has a similar 1-7 process; see the
# testing README.md for information on this.
def test_download(quiet=True):
    """Perform a test of the download task."""

    # (1) Initialize the download task.
    print('Running virtual download task.')
    task_init_args = {
            'storage_dir': 'unittests',
            'feeds': [],
            'quiet': quiet,
        }
    task = tasks.download.DownloadTask(**task_init_args)
    task.frequency = 5.00
    task.duration = 200
    task._log_file_path = os.path.join('unittests', 'download.log')

    # (2) Refresh the directory to be used for testing.
    file_system = task._file_system
    file_system.rmtree('unittests')

    # (3) Initialize the virtual objects.
    # For the download task we use a virtual downloader
    # with 3 feeds.
    vdownloader = virtualobjects.VirtualDownloader(clock_increment=5)
    vdownloader.set_bad_download_ratios(0.25, 0.25, 0.25)
    vdownloader.add_virtual_feed(refresh_period=30)
    vdownloader.add_virtual_feed(refresh_period=20)
    vdownloader.add_virtual_feed(refresh_period=15)
    vdownloader.attach_to(task)

    # (4) Write initial files to the file system.
    # For a download task there is nothing to do here as no task comes before.

    # (5) Perform the task.
    try:
        task.run()
    except Exception as e:
        print('Error: encountered exception in unit test run.')
        raise e

    # (6) Collect post-task data.

    # (6.1) collect stored data from the file system.
    files_data = utils._analyze_unittest_feeds(
        task._files_schema.list_downloaded_feeds(task.time())
        )

    # (6.2) collect information from the log file.
    log = log_templates_pb2.DownloadTaskLog()
    with open(task._log_file_path, 'rb') as f:
        log.ParseFromString(f.read())
    log_data = {}
    for i in range(len(log.num_downloaded)):
        feed_url = log.feeds[i].feed_url
        log_data[feed_url] = log.num_downloaded[i]

    # (7) Perform the comparisons.

    # Iterate over each feed
    row = '  {: <15} | {: <12} | {: <12} | {: <12}'
    for feed_url, server_data in vdownloader.responses.items():
        # Print the table heading
        print('  ----')
        print('  Virtual feed: {}'.format(feed_url))
        print(row.format('type', 'server', 'downloads', 'log file'))

        # TEST 1
        # First compare the feed information in the virtual server versus
        # the files on disk. In this case we can make seperate tests
        # for each of the three types of downloaded feed: valid, empty
        # or corrupt.
        t1 = True
        for feed_type in ['valid', 'corrupt', 'empty']:
            t1 = t1 and utils._compare(
                row,
                feed_type,
                server_data[feed_type],
                files_data[feed_url][feed_type],
                None
                )

        # TEST 2
        # Next compare the total downloads in the server vs log vs
        # downloaded files.
        server_total = (
            server_data['valid']
            + server_data['corrupt']
            + server_data['empty']
            )
        files_total = (
            files_data[feed_url]['valid']
            + files_data[feed_url]['corrupt']
            + files_data[feed_url]['empty']
            )
        t2 = utils._compare(
            row,
            'total',
            server_total,
            files_total,
            log_data[feed_url]
            )

        # TEST 3
        # Finally compare the number of failed downloads in the log versus
        # the virtual server.
        t3 = utils._compare(
            row,
            'failed download',
            server_data['failed'],
            None,
            log.num_cycles-log_data[feed_url]
            )

        # Print the pass/fail message.
        utils._pass_or_fail(t1 and t2 and t3)
