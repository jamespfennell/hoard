"""Realtime Aggregator Driver.

For command line usage, run:

    python3 realtimeaggregator.py -h
"""

import argparse
import importlib
import os
import tasks
import time
from tasks import tools
from tasks import settings
from tasks import exceptions

#
# (1) Parse the command line arguments using argparse
#
parser = argparse.ArgumentParser(description="Perform a realtime aggregator task")
parser.add_argument('task', help='the task to perform', choices=['download', 'filter', 'compress', 'archive', 'test'])
parser.add_argument("-f", "--frequency", help="in a download task: how often to perform a download cycle, in seconds", type=float, default=60)
parser.add_argument("-d", "--duration", help="in a download task: the length of time to run before closing, in seconds", type=float, default = 0)
parser.add_argument("-l", "--limit", help="in a filter, compress or archive task: the maximum number of files to process", type=int, default = -1)
parser.add_argument("-p", "--prefix", help="in an archive task: a string to prefix to the names of files being stored remotely", default = '')
parser.add_argument("-r", "--directory", help="the directory in which to store downloaded files and logs; default is ./", default = './')
parser.add_argument("-s", "--settings", help="the location of the feed and remote storage settings; default is remote_settings.py", default = 'remote_settings.py')
parser.add_argument("-q", "--quiet", help="suppress standard output", default = False, action='store_true')
args = parser.parse_args()


#
# (2) Import the feed and remote storage settings from the remote settings file
#
# This is a little bit delicate because the remote settings file is being imported as a Python module.
# We need to be sure (a) that the file exists, (b) that it's a valid Python module and (c) that it contains all the settings we need.

# (a) Use file location to make an importlib spec. If the result is None, the file could not be read.
spec = importlib.util.spec_from_file_location("", os.path.abspath(args.settings))
if spec == None:
    raise exceptions.UnreadableRemoteSettingsFileError('The remote settings file located at  "' + os.path.abspath(args.settings) + '" does not exist or cannot be read.')
remote = importlib.util.module_from_spec(spec)
# (b) Try to read the file as a Python module. There may be syntax errors.
try:
    spec.loader.exec_module(remote)
except SyntaxError:
    raise exceptions.InvalidRemoteSettingsFileError('The remote settings file located at  "' + os.path.abspath(args.settings) + '" has syntax errors.')
# (c) Now ensure that the settings file contains all the information we want.
# The feeds dictionary and using_remote_storage boolean must always exist; if the latter is True, then bucket storage settings are also needed.
try:
    remote.feeds
except AttributeError:
    raise exceptions.InvalidRemoteSettingsFileError('The remote settings file located at  "' + os.path.abspath(args.settings) + '" does not contain feeds information; expected a "feeds" lists.')
try:
    remote.using_remote_storage
except AttributeError:
    raise exceptions.InvalidRemoteSettingsFileError('The remote settings file located at  "' + os.path.abspath(args.settings) + '" does not contain a using_remote_storage boolean.')
# If the user wants to use remote storage, the settings have to exist.
if remote.using_remote_storage == True:
    try:
        remote.boto3_client_settings
        remote.bucket
        remote.global_prefix
    except AttributeError:
        raise exceptions.InvalidRemoteSettingsFileError('The remote settings file located at  "' + os.path.abspath(args.settings) + '" turns remote storage on, but does not provide remote storage settings: need "boto3_client_settings", "bucket" and "global_prefix".')

#
# (3) Do some options preprocessing: collect together the settings that will be passed to the Task class, and see if archive really needs to run.
#
if args.task == 'archive' and remote.using_remote_storage is False:
    print('Not using remote storage, so archive task has nothing to do.')
    exit()

task_init_args = {'root_dir' : args.directory,
        'feeds' : remote.feeds,
        'quiet' : args.quiet,
        'log_file_path' : 'logs/log.log'}


#
# (4) Start the master log. At this stage an task is basically guaranteed to run.
#
# We give every instance an task_id because tasks may overlap (for example, if a filter task runs while a download task is continuously running)
# and in the logs we will want to know which message relate to which tasks.
utc = tools.time.timestamp_to_utc_8601()
master_log = tools.logs.Log(args.directory + 'logs/master/master-' + utc[:-5] + '.log')
task_id = utc[-5:-1]

task_id = time.time()
task_id = str(int((task_id - int(task_id))*1000))

#
# (5) Perform the requested task. 
#

# The process for running the four standard tasks is quite similar, so we write the code together as much as possible to avoid duplicate code.
if args.task in ('download','filter','compress','archive'):
    task_init_args['log_file_path'] = args.directory + settings.log_dir[args.task] + args.task + '-' + tools.time.timestamp_to_utc_8601() + '.log'

    # Initialize the task class, depending on which task
    if args.task == 'download':
        task = tasks.download.DownloadTask(**task_init_args)
    elif args.task == 'filter':
        task = tasks.download.FilterTask(**task_init_args)
    elif args.task == 'compress':
        task = tasks.download.CompressTask(**task_init_args)
    elif args.task == 'archive':
        task = tasks.download.ArchiveTask(**task_init_args)

    # Pass additional variables
    task.frequency = args.frequency
    task.duration = args.duration
    task.limit = args.limit
    if args.task == 'archive':
        task.bucket = remote.bucket
        task.local_prefix = args.prefix
        task.global_prefix = remote.global_prefix
        task.boto3_settings = remote.boto3_client_settings

    # Write to the master log
    master_log.write('[' + task_id + '] Running ' + args.task + ' task.')
    master_log.write('[' + task_id + '] frequency=' + str(task.frequency) + ', duration=' + str(task.duration) + ', limit=' + str(task.limit) + '.')

    #Perform the task itself
    try:
        task.run()
    except KeyboardInterrupt:
        if args.task == 'download':
            task.stop('keyboard interrupt')
        else:
            raise KeyboardInterrupt
    except Exception as e:
        master_log.write('[' + task_id + '] Encountered exception: ' + repr(e))
        task.log.write(' Encountered exception: ' + repr(e))
        print('Encountered exception: ' + repr(e))

    # Report to the master log. This report depends on the task.
    if args.task == 'download':
        master_log.write('[' + task_id + '] Download task concluded: ran ' + str(task.n_cycles) + ' cycles and downloaded ' + str(task.n_downloads) + ' files')
    elif args.task == 'filter':
        master_log.write('[' + task_id + '] Filter task concluded: file counts: processed ' + str(task.n_total) + '; corrupt ' + str(task.n_corrupt)
                + '; copied: ' + str(task.n_copied) + '; duplicates: ' + str(task.n_skipped))
    elif args.task == 'compress':
        master_log.write('[' + task_id + '] Compress task concluded: created ' + str(task.n_compressed) + ' compressed file(s) corresponding to '
                + str(task.n_hours) + ' hours.')
    elif args.task == 'archive':
        master_log.write('[' + task_id + '] Archive task concluded: uploaded ' + str(task.n_uploaded) + ' files; failed to upload ' +str(task.n_failed) + '.')
    master_log.write('')

# Otherwise run the test task.
elif args.task == 'test':
    print('Running tests; quiet flag not honored!')
    task_init_args['quiet'] = False

    # Reset the test log.
    try:
        os.remove(args.directory + 'logs/test.log')
    except FileNotFoundError:
        pass
    task_init_args['log_file_path'] = args.directory + 'logs/test.log'

    # Run each task in order.
    print('(1) DOWNLOAD')
    task = tasks.download.DownloadTask(**task_init_args)
    task.frequency = 1
    task.duration = 2
    try:
        task.run()
    except KeyboardInterrupt:
        task.stop('keyboard interrupt')

    print('(2) FILTER')
    task = tasks.filter.FilterTask(**task_init_args)
    task.limit = -1
    task.file_access_lag = 0
    task.run()

    print('(3) COMPRESS')
    task = tasks.compress.CompressTask(**task_init_args)
    task.limit = -1
    task.force_compress = True
    task.run()

    print('(4) ARCHIVE')
    if remote.using_remote_storage is True:
        task = tasks.archive.ArchiveTask(**task_init_args)
        task.limit = -1
        task.bucket = remote.bucket
        task.local_prefix = args.prefix
        task.global_prefix = remote.global_prefix
        task.file_access_lag = 0
        task.boto3_settings = remote.boto3_client_settings
        task.run()
    else:
        print('Not using remote storage.')

    print('Tests complete.')




