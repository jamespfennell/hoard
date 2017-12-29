#!/usr/bin/python3

import requests
import time
import sys
from common import print_to_log, sortable_time, time_string, ensure_dir, MTAApiKey, urls, utc_8601

# Start the log file
(year, month, day, hours, mins, secs) = sortable_time()
logfile = 'logs/1-download.'+year+'-' + month + '-' + day + '-' + hours + '-' + mins + '-' + secs + '.log'
log = open(logfile, 'w')
print_to_log(log, 'New collecting session')


# Read the optional command line argument
if len(sys.argv) == 1:
    cutoff = False
    cutoff_time = 0
else:
    cutoff = True
    cutoff_time = float(sys.argv[1])*60

def elapsed_time(start, end):
    d = end-start
    d //= 1
    secs = str(int(d%60))
    d //= 60
    mins = str(int(d%60))
    d //= 60
    hours = str(int(d))
    if len(secs) == 1:
        secs = '0' + secs
    if len(mins) == 1:
        mins = '0' + mins
    return hours + ':' + mins + ':' + secs


#
# TODO:
# change the file name scheme for downloaded files (affects this and 2-filter.py) date/hour/line
# option to cut out after a certain amount of time
# 
#

print_to_log(log,'Collecting ' + str(len(urls)) + ' GTFS files:')
for (line, feed_id) in urls.items():
    print_to_log(log, ' - Lines ' + line + '; feed id: ' + feed_id)


start_time = time.time()
last_time = None
n_cycles = 0

try:
    while(True):

        n_cycles += 1
        print_to_log(log,'Beginning download cycle ' + str(n_cycles))

        t = (year, month, day, hour, mins, secs) = sortable_time()
        file_time = utc_8601(t)
        root_dir = '1-downloaded/' + year + '-'+month+'-'+day + '/' + hour + '/'
        print_to_log(log,'Downloading to directory ' + root_dir)

        ensure_dir(root_dir)
        count = 0
        for (lines,feed_id) in urls.items():
            ensure_dir(root_dir + lines + '/')
            try:
                f = open(root_dir + lines + '/' + lines + '-' + file_time + '-dt.gtfs', 'wb')
                r = requests.get('http://datamine.mta.info/mta_esi.php?key='+MTAApiKey+'&feed_id='+feed_id)
                f.write(r.content)
                f.close()
                count += 1
            except: #requests.exceptions.ConnectionError:
                print_to_log(log, 'Failed to download line ' + line + ' with feed id ' + feed_id)

        print_to_log(log, 'Download cycle ended with ' + str(count) + '/' + str(len(urls)) + ' files successfully downloaded')

        current_time = time.time()
        print('Download cycles completed: ' + str(n_cycles) + '; time since start: ' + elapsed_time(start_time, current_time))

        # If it has been running for over 24hrs, close (a new instance should be created by CRON)
        if current_time - start_time > cutoff_time:
            print('Closing because of elapsed time.')
            print_to_log(log, 'Closing because of elapsed time.')
            print_to_log(log, 'Download cycles completed: ' + str(n_cycles) + '; time since start: ' + elapsed_time(start_time, current_time))
            break
        time.sleep(10)


except KeyboardInterrupt:
    print()
    print('Closing because of keyboard interrupt.')
    print_to_log(log, 'Closing because of keyboard interrupt.')



log.close()


