import argparse
import importlib
import actions.download
parser = argparse.ArgumentParser(description="Perform a realtime aggregator action")


class InvalidRemoteSettingsFile(Exception):
    pass

parser.add_argument('action', help='the action to perform', choices=['download', 'filter', 'compress', 'archive', 'test'])
parser.add_argument("-f", "--frequency", help="in a download action: how often to perform a download cycle, in seconds", type=int, default=60)
parser.add_argument("-d", "--duration", help="in a download action: the length of time to run before closing, in seconds", type=int, default = 0)
parser.add_argument("-l", "--limit", help="in a filter, compress or archive action: the maximum number of files to process", type=int, default = -1)
parser.add_argument("-p", "--prefix", help="in an archive action: a string to prefix to the names of files being stored remotely", default = '')
parser.add_argument("-r", "--directory", help="the directory in which to store downloaded files and logs; default is ./", default = './')
parser.add_argument("-s", "--settings", help="the location of the feed and remote storage settings; default is remote_settings.py", default = 'remote_settings')
parser.add_argument("-q", "--quiet", help="suppress standard output", default = False, action='store_true')

args = parser.parse_args()


try:
    # Import the feed and remote storage settings
    remote = importlib.import_module(args.settings)
    user_feeds = remote.feeds
    if remote.using_remote_storage is True:
        pass

except Exception:
    raise InvalidRemoteSettingsFile


if args.action == 'download':

    action = actions.download.DownloadAction(frequency = args.frequency, 
                    duration = args.duration, directory = args.directory, feeds = user_feeds, quiet = args.quiet, log_file_path = './download.log')
    try:
        action.run()
    except KeyboardInterrupt:
        action.stop('keyboard interrupt')


elif args.action == 'filter':
    print('Going to filter')



#DownloadAction






