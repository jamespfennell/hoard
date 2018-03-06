"""Realtime Aggregator logs reader.
"""

if __name__ == "__main__" and __package__ is None:
    __package__ = "realtimeaggregator.logs"

import argparse
import importlib.util
import os
import shutil
import time
from realtimeaggregator.logs import log_templates_pb2


def main():
    parser = argparse.ArgumentParser(
            description="Realtime aggregator log utilities (beta)"
            )
    parser.add_argument(
            'action',
            help='the action to perform',
            choices=[
                'read'
                ]
            )
    parser.add_argument(
            'files',
            help='path(s) of the log file(s) to be read',
            type=str,
            nargs='+'
            )
    args = parser.parse_args()

    for path in args.files:
        print(
            '{dashes}[ {fp} ]{dashes}'.format(
                fp=path,
                dashes=('-' * (38 - len(path)//2))
                )
            )

        file_name = os.path.basename(path)
        log_type = file_name[:file_name.find('-')]

        log_object_by_type = {
            'download': log_templates_pb2.DownloadTaskLog,
            'filter': log_templates_pb2.FilterTaskLog,
            'compress': log_templates_pb2.CompressTaskLog,
            'archive': log_templates_pb2.ArchiveTaskLog
            }
        if log_type not in log_object_by_type:
            print('Error: cannot detect log type for file {}.'.format(path))
            continue

        log = log_object_by_type[log_type]()
        try:
            with open(path, 'rb') as f:
                log.ParseFromString(f.read())
        except FileNotFoundError:
            print('File {} does not exist!'.format(path))
            continue

        print(log)


if __name__ == '__main__':
    main()
