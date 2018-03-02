"""This module contains internal settings."""

downloaded_dir = 'store/downloaded/'
download_log_dir = 'logs/download/'
filtered_dir = 'store/filtered/'
downloaded_time_dir = 'store/filtered/downloaded_time/'
filter_log_dir = 'logs/filter/'
compressed_dir = 'store/compressed/'
compress_log_dir = 'logs/compress/'

log_dir = {}
for action in ['download', 'filter', 'compress', 'archive']:
    log_dir[action] = 'logs/{}/'.format(action)
