

import glob
import re
import time
import os
from common import print_to_log, sortable_time, time_string, ensure_dir, MTAApiKey, urls
from shutil import copyfile, rmtree
import tarfile



    



# Start the log file
(year, month, day, hours, mins, secs) = sortable_time()
logfile = 'logs/3-compress.'+year+'-' + month + '-' + day + '-' + hours + '-' + mins + '-' + secs + '.log'
log = open(logfile, 'w')
print_to_log(log, 'New compression session')

# Check in the 1-downloaded directory for empty sub-directories; such a directory is a candidate for compression
files = glob.glob('1-downloaded/[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/[0-9][0-9]')    
n_compressed = 0

for directory in files:
    # See if the directory is empty
    # If it is, this is a candidate for compression
    count = 0
    for lines in urls.keys():
        try:
            count += len(os.listdir(directory + '/' + lines))
        except FileNotFoundError:
            pass
        #print(directory + '/' + lines)
        #print( len([name for name in os.listdir(directory + '/' + lines)]) )

    # If the directory is not empty, skip
    if count > 0:
        continue

    # Read the date and hour from the directory string
    i1 = directory.find('/')
    i2 = directory.find('/',i1+1)
    date = directory[i1+1:i2]
    hour = directory[i2+1:]
    print_to_log(log, 'Potentially compressing files corresponding to hour ' + date + 'T' + hour)

    # Check that there are some files in the filtered directory
    filtered_dir = '2-filtered/' + date + '/' + hour + '/'
    if not os.path.isdir(filtered_dir):
        os.rmdir(directory)
        print_to_log(log, 'Filtered directory empty, so skipped; original directory removed')
        continue

    n_compressed += 1

    # Create the directory into which all of the compressed files will be put
    compressed_dir = '3-compressed/' + date + '/' + hour + '/'
    ensure_dir(compressed_dir)

    # Now iterate over each of the lines
    for line in urls.keys():
        source_dir = filtered_dir + line + '/'
        target_file = compressed_dir + line + '-' + date + 'T' + hour + '.tar.bz2'
        print_to_log(log, 'Compressing files in ' + source_dir)
        print_to_log(log, '    into ' + target_file)

        # If the tar file already exists, extract it into the filtered directory first
        # The cumulative effect will be that the new filtered files will be `appended' to the tar file
        if os.path.isfile(target_file):
            print_to_log(log, 'File ' + target_file + ' already exists; extracting it first')
            tar_file = tarfile.open(target_file, 'r:bz2')
            tar_file.extractall(source_dir)
            tar_file.close()
            os.remove(target_file)

        # Compress the source directory into the archive
        tar_file = tarfile.open(target_file, 'x:bz2')
        tar_file.add(source_dir, arcname='')
        tar_file.close()
        print_to_log(log, 'Compressed')

        # Delete the source directory
        rmtree(source_dir)
        print_to_log(log, 'Removed directory ' + source_dir)


    # Remove the original directory in 1-downloaded; recall the existence of this empty directory was the flag to compress
    # Also remove the filtered directory
    for lines in urls.keys():
        try:
            os.rmdir(directory + '/' + lines)
        except FileNotFoundError:
            pass
        try:
            os.rmdir(filtered_dir + lines)
        except FileNotFoundError:
            pass

    os.rmdir(directory)
    os.rmdir(filtered_dir)

    print_to_log(log, 'Removed directory ' + directory)
    print_to_log(log, 'Removed directory ' + filtered_dir)

    # Potentially remove the directory for the date, if it's empty
    try:
        os.rmdir('1-downloaded/' + date + '/')
        print_to_log(log, 'Removed directory 1-downloaded/' + date + '/')
    except:
        pass

    try:
        os.rmdir('2-filtered/' + date + '/')
        print_to_log(log, 'Removed directory 2-filtered/' + date + '/')
    except:
        pass



    # See if the compression limit has been reached
    print('Compressed hour: ' + date + 'T' + hour)
    if n_compressed > 20:
        print_to_log(log, 'Reached compression limit of 20 hours, ending')
        print('Reached compression limit of 20 hours, ending')
        print('Run again to compress more hours')
        break



print_to_log(log, 'Compressed files corresponding to ***' + str(n_compressed) + '*** hour(s)')

