"""Realtime Aggregator Driver."""

import argparse
import importlib
import os
import actions
from actions import tools
from actions import settings

# Parse the command line arguments using argparse
parser = argparse.ArgumentParser(description="Perform a realtime aggregator action")
parser.add_argument('action', help='the action to perform', choices=['download', 'filter', 'compress', 'archive', 'test','cleanlogs'])
parser.add_argument("-f", "--frequency", help="in a download action: how often to perform a download cycle, in seconds", type=float, default=60)
parser.add_argument("-d", "--duration", help="in a download action: the length of time to run before closing, in seconds", type=float, default = 0)
parser.add_argument("-l", "--limit", help="in a filter, compress or archive action: the maximum number of files to process", type=int, default = -1)
parser.add_argument("-p", "--prefix", help="in an archive action: a string to prefix to the names of files being stored remotely", default = '')
parser.add_argument("-r", "--directory", help="the directory in which to store downloaded files and logs; default is ./", default = './')
parser.add_argument("-s", "--settings", help="the location of the feed and remote storage settings; default is remote_settings.py", default = 'remote_settings.py')
parser.add_argument("-q", "--quiet", help="suppress standard output", default = False, action='store_true')
args = parser.parse_args()

# Try to read in the remote settings file.
# There is wide scope here for something to go wrong.
class InvalidRemoteSettingsFile(Exception):
    pass
try:
    # Import the feed and remote storage settings
    spec = importlib.util.spec_from_file_location("", os.path.abspath(args.settings))
    remote = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(remote)
except Exception:
    raise InvalidRemoteSettingsFile


# Open up the master log
# We give every instance an action_id because actions may overlap and in the logs we will want to know which
# message relate to which actions.
utc = tools.time.timestamp_to_utc_8601()
master_log = tools.logs.Log(args.directory + 'logs/master/master-' + utc[:-5] + '.log')
action_id = utc[-5:-1]

# Prepare the arguments to the actions class
actions_init_args = {'root_dir' : args.directory,
	'feeds' : remote.feeds,
	'quiet' : args.quiet,
	'log_file_path' : 'logs/log.log'}

#What we do now depends on the action requested.
if args.action == 'download':
    
    actions_init_args['log_file_path'] = args.directory + settings.download_log_dir + 'download-' + tools.time.timestamp_to_utc_8601() + '.log'
    action = actions.download.DownloadAction(**actions_init_args)
    action.frequency = args.frequency
    action.duration = args.duration
    master_log.write('[' + action_id + '] Running download action: downloading every ' + str(action.frequency) + ' seconds for ' + str(args.duration) + ' seconds.')
    try:
        action.run()
    except KeyboardInterrupt:
        action.stop('keyboard interrupt')
    master_log.write('[' + action_id + '] Download action concluded: ran ' + str(action.n_cycles) + ' cycles and downloaded ' + str(action.n_downloads) + ' files')
    master_log.write('')

elif args.action == 'filter':

    actions_init_args['log_file_path'] = args.directory + settings.filter_log_dir + 'filter-' + tools.time.timestamp_to_utc_8601() + '.log'
    action = actions.filter.FilterAction(**actions_init_args)
    action.limit = args.limit
    master_log.write('[' + action_id + '] Running filter action')
    action.run()
    master_log.write('[' + action_id + '] Filter action concluded: file counts: processed ' + str(action.n_total) + '; corrupt ' + str(action.n_corrupt)
            + '; copied ' + str(action.n_copied) + '; skipped ' + str(action.n_skipped) + '.')
    master_log.write('')

elif args.action == 'compress':

    actions_init_args['log_file_path'] = args.directory + settings.compress_log_dir + 'compress-' + tools.time.timestamp_to_utc_8601() + '.log'
    action = actions.compress.CompressAction(**actions_init_args)
    action.limit = args.limit
    master_log.write('[' + action_id + '] Running compress action')
    action.run()
    master_log.write('[' + action_id + '] Compress action concluded: created ' + str(action.n_compressed) + ' compressed file(s) corresponding to '
            + str(action.n_hours) + ' hour(s).')
    master_log.write('')

elif args.action == 'archive':

    if remote.using_remote_storage is True:
        actions_init_args['log_file_path'] = args.directory + settings.archive_log_dir + 'archive-' + tools.time.timestamp_to_utc_8601() + '.log'
        action = actions.archive.ArchiveAction(**actions_init_args)
        action.limit = args.limit
        action.bucket = remote.bucket
        action.local_prefix = args.prefix
        action.global_prefix = remote.global_prefix
        action.boto3_settings = remote.boto3_client_settings
        master_log.write('[' + action_id + '] Running archive action')
        action.run()
        master_log.write('[' + action_id + '] Archive action concluded: uploaded ' + str(action.n_uploaded) + ' files; failed to upload ' +str(action.n_failed) + '.')
        master_log.write('')
    else:
        print('  Not using remote storage.')

elif args.action == 'test':

    try:
        os.remove(args.directory + 'logs/test.log')
    except FileNotFoundError:
        pass
    actions_init_args['log_file_path'] = args.directory + 'logs/test.log'

    print('(1) DOWNLOAD')
    action = actions.download.DownloadAction(**actions_init_args)
    action.frequency = 1
    action.duration = 2
    try:
        action.run()
    except KeyboardInterrupt:
        action.stop('keyboard interrupt')

    print('(2) FILTER')
    action = actions.filter.FilterAction(**actions_init_args)
    action.limit = -1
    action.file_access_lag = 0
    action.run()

    print('(3) COMPRESS')
    action = actions.compress.CompressAction(**actions_init_args)
    action.limit = -1
    action.force_compress = True
    action.run()

    print('(4) ARCHIVE')
    if remote.using_remote_storage is True:
        action = actions.archive.ArchiveAction(**actions_init_args)
        action.limit = -1
        action.bucket = remote.bucket
        action.local_prefix = args.prefix
        action.global_prefix = remote.global_prefix
        action.file_access_lag = 0
        action.boto3_settings = remote.boto3_client_settings
        action.run()
    else:
        print('Not using remote storage.')





