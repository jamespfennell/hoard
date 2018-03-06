if __name__ == "__main__" and __package__ is None:
    __package__ = "realtimeaggregator.tests"

from realtimeaggregator.tests import test_download
from realtimeaggregator.tests import test_filter
from realtimeaggregator.tests import test_compress
from realtimeaggregator.tests import test_archive
import argparse


def main():
    parser = argparse.ArgumentParser(
            description="Perform a realtime aggregator test")
    parser.add_argument(
            'test',
            help='the test to perform',
            choices=['download', 'filter', 'compress', 'archive', 'all'],
            default='all'
            )
    parser.add_argument(
            "-q", "--quiet",
            help="suppress standard output in tasks",
            default=False,
            action='store_true')
    args = parser.parse_args()

    if args.test == 'download' or args.test == 'all':
        test_download.test_download(quiet=args.quiet)
    if args.test == 'filter' or args.test == 'all':
        test_filter.test_filter(quiet=args.quiet)
    if args.test == 'compress' or args.test == 'all':
        test_compress.test_compress(quiet=args.quiet)
    if args.test == 'archive' or args.test == 'all':
        test_archive.test_archive(quiet=args.quiet)


def print_download_task_log(file_path):
    log = log_templates_pb2.DownloadTaskLog()
    with open(file_path, 'rb') as f:
        log.ParseFromString(f.read())
    print(log)


def print_filter_task_log(file_path):
    log = log_templates_pb2.FilterTaskLog()
    with open(file_path, 'rb') as f:
        log.ParseFromString(f.read())
    print(log)


def print_compress_task_log(file_path):
    log = log_templates_pb2.CompressTaskLog()
    with open(file_path, 'rb') as f:
        log.ParseFromString(f.read())
    print(log)


def print_archive_task_log(file_path):
    log = log_templates_pb2.ArchiveTaskLog()
    with open(file_path, 'rb') as f:
        log.ParseFromString(f.read())
    print(log)


if __name__ == '__main__':
    main()
