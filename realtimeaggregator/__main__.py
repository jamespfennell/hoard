"""Realtime Aggregator Driver.
"""

if __name__ == "__main__" and __package__ is None:
    __package__ = "realtimeaggregator"

import argparse
import importlib.util
import os
import shutil
import time
from realtimeaggregator import tasks


def main():
    #
    # (1) Parse the command line arguments using argparse
    #
    parser = argparse.ArgumentParser(
            description="Perform a realtime aggregator task")
    parser.add_argument(
            'task',
            help='the task to perform',
            choices=[
                'download',
                'filter',
                'compress',
                'archive',
                'testrun',
                'makersf',
                'makectf'
                ]
            )
    parser.add_argument(
            "-q", "--quiet",
            help="suppress standard output",
            default=False,
            action='store_true')
    parser.add_argument(
            "-r", "--directory",
            help="the directory in which to store downloaded "
                 "files and logs; default is ./",
            type=str,
            default='./')
    parser.add_argument(
            "-s", "--settings",
            help="the location of the feed and remote storage settings; "
                 "default is ./remote_settings.py",
            type=str,
            default='remote_settings.py')
    parser.add_argument(
            '-f', "--frequency",
            help="in a download task: how often to perform "
                 "a download cycle, in seconds",
            type=float,
            default=10)
    parser.add_argument(
            "-d", "--duration",
            help="in a download task: the length of time to run "
                 "before closing, in seconds",
            type=float,
            default=60)
    parser.add_argument(
            "-l", "--limit",
            help="in a filter, compress or archive task: the maximum "
                 "number of files to process",
            type=int,
            default=-1)
    parser.add_argument(
            "-c", "--compressall",
            help="in a compress task, compress all filtered files regardless "
                 "of whether a compress flag has been set",
            default=False,
            action='store_true')
    parser.add_argument(
            "-p", "--prefix",
            help="in an archive task: a string to prefix to the "
                 "names of files being stored remotely",
            type=str,
            default='')
    args = parser.parse_args()

    #
    # (2) Import the remote settings file
    #
    remote_settings = import_remote_settings_file(args.settings)

    task_init_args = {
        'storage_dir': args.directory,
        'feeds': remote_settings.feeds,
        'quiet': args.quiet
        }

    #
    # (3) Perform the desired task.
    #
    if args.task == 'download':
        task = tasks.download.DownloadTask(**task_init_args)
        task.frequency = args.frequency
        task.duration = args.duration
        task.run()

    if args.task == 'filter':
        task = tasks.filter.FilterTask(**task_init_args)
        task.limit = args.limit
        task.run()

    if args.task == 'compress':
        task = tasks.compress.CompressTask(**task_init_args)
        task.limit = args.limit
        task.compress_all = args.compressall
        task.run()

    if args.task == 'archive':
        if not remote_settings.using_remote_storage:
            print('Remote settings file says remote storage not being used.')
            print('Nothing to do.')
            exit()
        task = tasks.archive.ArchiveTask(**task_init_args)
        task.limit = args.limit
        task.init_boto3_remote_storage(
            bucket=remote_settings.bucket,
            local_prefix=args.prefix,
            global_prefix=remote_settings.global_prefix,
            client_settings=remote_settings.boto3_client_settings
            )
        task.run()

    if args.task == 'testrun':
        task_init_args['quiet'] = False

        task = tasks.download.DownloadTask(**task_init_args)
        task.frequency = 1
        task.duration = 5
        try:
            task.run()
        except KeyboardInterrupt:
            pass

        print('-'*70)

        task = tasks.filter.FilterTask(**task_init_args)
        task.limit = -1
        task.set_file_access_lag(0)
        task.run()

        print('-'*70)

        task = tasks.compress.CompressTask(**task_init_args)
        task.limit = -1
        task.set_file_access_lag(0)
        task.compress_all = True
        task.run()

        print('-'*70)

        if not remote_settings.using_remote_storage:
            print('Remote settings file says remote storage not being used.')
            print('Nothing to do.')
            exit()
        task = tasks.archive.ArchiveTask(**task_init_args)
        task.limit = -1
        task.set_file_access_lag(0)
        task.init_boto3_remote_storage(
            bucket=remote_settings.bucket,
            local_prefix=args.prefix,
            global_prefix=remote_settings.global_prefix,
            client_settings=remote_settings.boto3_client_settings
            )
        task.run()

    if args.task == 'makersf':
        if os.path.isfile(args.settings):
            print('File {} already exists.'.format(args.settings))
            answer = input('Overwrite [y/n]? ')
            if answer != 'y' and answer != 'Y':
                print('Not overwriting.')
                return
            print('Overwriting.')

        script_dir = os.path.dirname(os.path.realpath(__file__))
        original_path = os.path.join(script_dir, 'remote_settings.py')       
        shutil.copyfile(original_path, args.settings)

    if args.task == 'makectf':
        if args.settings == 'remote_settings.py':
            args.settings = 'schedules.crontab'
        if os.path.isfile(args.settings):
            print('File {} already exists.'.format(args.settings))
            answer = input('Overwrite [y/n]? ')
            if answer != 'y' and answer != 'Y':
                print('Not overwriting.')
                return
            print('Overwriting.')

        script_dir = os.path.dirname(os.path.realpath(__file__))
        original_path = os.path.join(script_dir, 'schedules.crontab')       
        shutil.copyfile(original_path, args.settings)


def import_remote_settings_file(file_path):

    # This is a little bit delicate because the remote settings file is being
    # imported as a Python module.
    # We need to be sure (a) that the file exists, (b) that it's a valid Python
    # module and (c) that it contains all the settings we need.

    # (a) Use file location to make an importlib spec. If the result is None,
    #     the file could not be read.
    spec = importlib.util.spec_from_file_location(
            '', os.path.abspath(file_path))
    if spec is None:
        raise exceptions.UnreadableRemoteSettingsFileError(
                'The remote settings file located at  "' +
                os.path.abspath(args.settings) +
                '" does not exist or cannot be read.')
    remote = importlib.util.module_from_spec(spec)

    # (b) Try to read the file as a Python module. There may be syntax errors.
    try:
        spec.loader.exec_module(remote)
    except SyntaxError:
        raise exceptions.InvalidRemoteSettingsFileError(
                'The remote settings file located at  "' +
                os.path.abspath(args.settings) +
                '" has syntax errors.')

    # (c) Now ensure that the settings file contains all the information we
    #     want.
    #     The feeds dictionary and using_remote_storage boolean must always
    #     exist; if the latter is True, then bucket storage settings are also
    #     needed.
    try:
        remote.feeds
    except AttributeError:
        raise exceptions.InvalidRemoteSettingsFileError(
            'The remote settings file located at  "' +
            os.path.abspath(args.settings) +
            '" does not contain feeds information; expected a "feeds" lists.')
    try:
        remote.using_remote_storage
    except AttributeError:
        raise exceptions.InvalidRemoteSettingsFileError(
                'The remote settings file located at  "' +
                os.path.abspath(args.settings) +
                '" does not contain a using_remote_storage boolean.')
    # If the user wants to use remote storage, the settings have to exist.
    if remote.using_remote_storage:
        try:
            remote.boto3_client_settings
            remote.bucket
            remote.global_prefix
        except AttributeError:
            raise exceptions.InvalidRemoteSettingsFileError(
                    'The remote settings file located at  "' +
                    os.path.abspath(args.settings) +
                    '" turns remote storage on, but does not provide '
                    'remote storage settings: need "boto3_client_settings", '
                    '"bucket" and "global_prefix".')

    return remote


if __name__ == '__main__':
    main()
