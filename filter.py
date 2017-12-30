"""
This module filters the files have been downloaded by the download moving, removing duplicates. Files are moved to
a new subdirectory and renamed to that the timestamp given by the agency is expressed in the file name.
Command line usage:

    python3 filter.py <max_inspections (optional)>

where:
max_inspection -- an integer representing the maximum number of downloaded files to inspect and process. This might be useful
                    if you're manually filtering a large number of files and want to do it carefully and in stages.

"""
import calendar, glob, os, time
from common_code import *
from user_settings import *
from shutil import copyfile
from google.transit import gtfs_realtime_pb2


# Start the log file
logfile_path =  filter_log_dir + 'filter-' + timestamp_to_utc_8601() + '.log'
log = open(logfile_path, 'w')
print_to_log(log, 'New filtering session begun.')


# Grab the optional input argument
# If there's a problem, note it to the logs and exit







print_to_log(log, 'Finding all .gtfs files in ./1-downloaded/')

current_time = time.time()

files = glob.iglob('1-downloaded/*/*/*/*.gtfs')    

n_corrupt = 0
n_copied = 0
n_skipped = 0

for file_name in files:
    # Use the file name to infer the line and the download time (given in UTC 8601(
    a = file_name.rfind('/')
    b = file_name.find('-',a)
    c = file_name.rfind('-')
    line = file_name[a+1:b]
    utc = file_name[b+1:c]
    elapsed = current_time - utc_8601_to_timestamp(utc)

    # If the file was downloaded in the last two minutes, skip it to avoid file IO interaction with module 1
    if elapsed < 120:
        continue

    # Otherwise open up the file
    f = open(file_name, 'rb')
    data = f.read()
    f.close()
    # Try to read it as a GTFS file
    # If there is an error reading it, the file is corrupt
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(data)
    except:
        print_to_log(log, 'Corrupt file (GTFS load error): ' + file_name)
        n_corrupt += 1
        os.remove(file_name)
        continue

    # Read the time stamp on the GTFS file
    # If the time stamp is 0, the file is corrupt
    dataset_time = feed.header.timestamp
    if dataset_time == 0:
        print_to_log(log, 'Corrupt file (timestamp is 0): ' + file_name)
        n_corrupt += 1
        os.remove(file_name)
        continue

    # Otherwise, the file is a valid GTFS file
    # Calculate the directory and file it is to be copied to in ./2-filtered/
    t = sortable_time(time.gmtime(dataset_time))
    target_dir = '2-filtered/' + t[0] + '-' + t[1] + '-' + t[2] + '/' + t[3] + '/' + line + '/'
    target_file = line + '-' + utc_8601(t) + '.gtfs'
    ensure_dir(target_dir)

    # If the target file does not exists, copy it
    # Otherwise this is a duplicate GTFS file and can be skipped
    if not os.path.isfile(target_dir + target_file):
        print_to_log(log, 'Copying: ' + file_name)
        print_to_log(log, '    to ' + target_dir + target_file)
        copyfile(file_name, target_dir + target_file)
        n_copied += 1
    else:
        print_to_log(log, 'Skipping (duplicate):' + file_name)
        n_skipped += 1

    # Remove the original file
    os.remove(file_name)


    if n_copied + n_skipped >= 10000:
        print_to_log(log, 'Copy threshold reached; closing')
        print('Copy threshold reached; closing.')
        print('Run again to filter more files.')
        break


print_to_log(log, 'Processed ' + str(n_corrupt + n_copied + n_skipped) + ' files')
print_to_log(log, str(n_corrupt) + ' corrupt files')
print_to_log(log, str(n_copied) + ' copied files')
print_to_log(log, str(n_skipped) + ' skipped files (duplicates of copied files)')




