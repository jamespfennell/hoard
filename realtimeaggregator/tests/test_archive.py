import os
import random
import requests.exceptions
import time
from . import utils
from . import virtualobjects
from .. import tasks
from ..logs import log_templates_pb2


# The testing process for all tasks has a similar 1-7 process; see the
# testing README.md for information on this.
def test_archive(quiet=True):

    # (1) Initialize the filter task.
    print('Running virtual archive task.')
    task_init_args = {
            'storage_dir': 'unittests',
            'feeds': [],
            'quiet': quiet,
        }
    task = tasks.archive.ArchiveTask(**task_init_args)
    task.limit = -1
    task.file_access_lag = 0
    task._log_file_path = os.path.join('unittests', 'archive.log')

    # (2) Refresh the directory to be used for testing.
    file_system = task._file_system
    file_system.rmtree('unittests')

    # (3) Initialize the virtual objects.
    # For the archive task we use a virtual downloader and a virtual
    # transferer

    # Initialize the virtual downloader with 3 feeds.
    # The clock increment is greater than all refresh_periods;
    # this ensures no duplicates. (Not that it really matters.)
    # The downloader will have 15 feeds
    vdownloader = virtualobjects.VirtualDownloader(
        clock_init=(time.time() - (time.time() % 3600) - 2000),
        clock_increment=40
        )
    vdownloader.set_bad_download_ratios(0, 0, 0.25)
    for _ in range(15):
        vdownloader.add_virtual_feed(refresh_period=25)
    vdownloader.attach_to(task)

    # Initialize the virtual transferer
    vtransferer = virtualobjects.VirtualTransferer()
    vtransferer.objects_handled = set()
    vtransferer.attach_to(task)

    # (4) Write initial files to the file system.
    # For simplicity, all downloads will be from the same hour for this task.
    test_time = time.time()

    # We are going to run the feed downloads twice.
    # After the first run, the feeds will will be compressed and transfered
    # to the virtual storage.
    # The second run feeds will remain on disk and will be the compressed
    # files the task has to deal with.
    # This setup is to test the archive task's ability to merge preexisting
    # files in bucket storage with new files.
    first_run = True
    # The feed downloads will be downloaded into a subdirectory of tmp_dir
    # to be compressed
    tmp_dir = os.path.join('unittests', 'tmp')
    for _ in range(2):
        for (feed_id, feed_url, _, _) in vdownloader.feeds:
            # If this is the first run, randomly do nothing.
            if first_run and random.random() <= 0.5:
                continue
            source_dir = os.path.join(tmp_dir, feed_id)

            # Download 30 of each feed.
            for k in range(30):
                try:
                    vdownloader.download(
                        feed_url,
                        os.path.join(
                            source_dir,
                            '{}-{}{}'.format(feed_id, k, first_run)
                            ),
                        file_system
                        )
                except requests.exceptions.RequestException:
                    # The virtual downloader throws random errors.
                    # This way each feed will have a different number of
                    # of feed downloads
                    pass

            # Now compress the download feeds.
            target_file_path = task._files_schema.compressed_file_path(
                test_time,
                feed_id
                )
            target_key = task._files_schema.archived_file_key(
                test_time,
                feed_id
                )
            file_system.dir_to_tar_file(source_dir, target_file_path)

            # If this is the first run, 'upload' the files.
            if first_run:
                vtransferer.upload(
                    target_file_path,
                    task._file_system,
                    target_key,
                    remove_original=True
                    )

        # After the first run, mark finished.
        first_run = False
        # Introduce upload errors for the task run.
        vtransferer.error_rate = 0.4

    # (5) Perform the task.
    try:
        task.run()
    except Exception as e:
        print('Error: encountered exception in unit test run.')
        raise e

    # (6) Collect post-task data.

    # (6.1) collect stored data from the file system.
    # There is not data on disk in this case: all of the data is in
    # the virtual bucket storage.

    # (6.2) collect information from the log file.
    log = log_templates_pb2.ArchiveTaskLog()
    log_succesful_feeds = set()
    log_data = {'success': 0, 'download_error': 0, 'upload_error': 0}
    with open('unittests/archive.log', 'rb') as f:
        log.ParseFromString(f.read())
    for i in range(len(log.uploads)):
        feed_id = log.uploads[i].feed_id
        if log.uploads[i].success:
            log_succesful_feeds.add(feed_id)
            log_data['success'] += 1
        elif log.uploads[i].download_error.name != '':
            log_data['download_error'] += 1
        else:
            log_data['upload_error'] += 1

    # (7) Perform the comparisons.
    row = '  {: <19} | {: <12} | {: <12} | {: <12}'
    # Print the table heading
    print('  ----')
    print(row.format('type', 'v server', 'v transferer', 'log file'))

    # TEST 1
    # Compare the total number of uploads attempted.
    t1 = utils._compare(
        row,
        'upload attempts',
        len(vdownloader.feeds),
        len(vtransferer.objects_handled),
        len(log.uploads)
        )

    # TEST 2
    # Compare the total number of upload attempts that experienced a
    # download error. The virtual server doesn't see this.
    t2 = utils._compare(
        row,
        'download errors',
        None,
        vtransferer.num_download_errors,
        log_data['download_error']
        )

    # TEST 3
    # Compare the total number of upload attempts that experienced an
    # upload error. The virtual server doesn't see this.
    t3 = utils._compare(
        row,
        'upload errors',
        None,
        vtransferer.num_upload_errors,
        log_data['upload_error']
        )

    # Print the pass or fail notice for this batch of tests.
    utils._pass_or_fail(t1 and t2 and t3)

    # Next we perform feed by feed tests for all of the succesful feeds.
    # For these feeds we will 'download' the feeds from the virtual storage
    # and ensure the correct number of feeds are present.
    print(
        '  For each succesful feed, will now verify '
        'that the feeds were stored correctly'
        )
    print(row.format('feed id', 'v server', 'on disk', ''))

    # Print the table heading
    print('  ----')
    vtransferer.error_rate = 0
    t4 = True

    # Iterate over each feed, ignoring ones which weren't succesful.
    for (feed_id, feed_url, _, _) in vdownloader.feeds:
        if feed_id not in log_succesful_feeds:
            continue

        # Determine the source and target.
        source_key = task._files_schema.archived_file_key(test_time, feed_id)
        target_file_name = os.path.basename(
            task._files_schema.compressed_file_path(
                test_time,
                feed_id
                )
            )
        target_dir = os.path.join(tmp_dir, feed_id)
        target_file_path = os.path.join(target_dir, target_file_name)

        # Download and extract.
        vtransferer.download(source_key, target_file_path, task._file_system)
        file_system.tar_file_to_dir(target_file_path, target_dir)

        # TEST 4
        # Ensure the number of feeds produced by the server is the same
        # as the number in remote object storage
        t4 = t4 and utils._compare(
            row,
            feed_id,
            file_system.num_files_in_dir(target_dir),
            vdownloader.responses[feed_url]['valid'],
            None
            )

    utils._pass_or_fail(t4)
