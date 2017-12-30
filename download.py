"""
This module downloads real time feeds described in user_settings.py at a given frequency for for a certain length of time
Command line usage:

    python3 download.py <frequency> <cutoff_time>

where:
frequency -- an integer representing the frequency in seconds at which the feeds should be downloaded.
cutoff_time -- an integer representing the number of seconds the module should run before closing. The idea of having a cutoff is
                to run the module every (say) 5 minutes for 6 minutes; if there is a failure in a given instance then only 5
                minutes of data will be lost before the next instance starts. By starting multiple, overlapping instances (for example,
                start a 15 minute long instance every 5 minutes) one can introduce greater redundancy. 
"""
import requests, sys, time
from common_code import *
from user_settings import*

# Start the log file
logfile_path =  download_log_dir + 'download-' + timestamp_to_utc_8601() + '.log'
log = open(logfile_path, 'w')
print_to_log(log, 'New downloading session begun.')

# Grab the input arguments
# If there's a problem, note it to the logs and exit
try:
    if len(sys.argv) != 3:
        raise IndexError
    frequency = int(sys.argv[1])
    cutoff_time = int(sys.argv[2])

except IndexError:
    print('Invalid number of input arguments passed: expected 2, received ' + str(len(sys.argv)-1))
    print_to_log(log, 'Invalid number of input arguments passed: expected 2, received ' + str(len(sys.argv)-1))
    print_to_log(log, 'The two input arguments should be integers representing the frequency and cutoff_time respectively.')
    print_to_log(log, 'These are the arguments that were passed:')
    print_to_log(log, str(sys.argv[1:]))
    exit()

except ValueError:
    print('Invalid input arguments passed: expected 2 integers, received something else.')
    print_to_log(log, 'Invalid input arguments passed: expected 2 integers, received something else.')
    print_to_log(log, 'The two input arguments should be integers representing the frequency and cutoff_time respectively.')
    print_to_log(log, 'These are the arguments that were passed:')
    print_to_log(log, str(sys.argv[1:]))
    exit()

print_to_log(log, 'Collecting every ' + str(frequency) + ' seconds for ' + str(cutoff_time) + ' seconds.')

# Log the feeds we're going to aggregate
print_to_log(log,'Collecting the following ' + str(len(feeds)) + ' feeds:')
for (uid, url, func) in feeds:
    print_to_log(log, ' - UID: ' + uid + '; from URL: ' + url)


start_time = time.time()
n_cycles = 0
# The try statement here is primarily to allow for keyboard interupts to be logged.
try:
    # Begin a download cycle
    while(True):
        n_cycles += 1
        print_to_log(log,'Beginning download cycle ' + str(n_cycles))

        # Establish the directory into which the feeds will be downloaded
        t = (year, month, day, hour, mins, secs) = timestamp_to_data_list()
        file_time = timestamp_to_utc_8601()
        root_dir = downloaded_dir + year + '-'+month+'-'+day + '/' + hour + '/'
        print_to_log(log,'Downloading to directory ' + root_dir)
        ensure_dir(root_dir)
        count = 0

        # Iterate through every feed and download it.
        # The try/except block here is intentionally broad: in the worst case, only the present download should be abandoned, the program
        # should continue on no matter what happens locally inside here.
        for (uid, url, func) in feeds:
            ensure_dir(root_dir + uid + '/')
            try:
                f = open(root_dir + uid + '/' + uid + '-' + file_time + '-dt.gtfs', 'wb')
                r = requests.get(url)
                f.write(r.content)
                f.close()
                count += 1
            except Exception: 
                print_to_log(log, 'Failed to download feed with UID ' + uid)

        
        # Log the results of this download cycle
        print_to_log(log, 'Download cycle ended with ' + str(count) + '/' + str(len(feeds)) + ' feeds successfully downloaded')
        print('Cycle ' + str(n_cycles) + ': ' + str(count) + '/' + str(len(feeds)) + ' feeds downloaded.')
        current_time = time.time()
        print_to_log(log, 'Download cycles completed: ' + str(n_cycles) + '; time since start: ' + str(int(-start_time +current_time)) + ' seconds.')

        # If the module has been running for longer than the cutoff time, end it
        if current_time - start_time > cutoff_time:
            print('Closing because of elapsed time.')
            print_to_log(log, 'Closing because of elapsed time.')
            break

        # Pause for frequency seconds
        time.sleep(frequency)


except KeyboardInterrupt:
    print()
    print('Closing because of keyboard interrupt.')
    print_to_log(log, 'Closing because of keyboard interrupt.')



log.close()


