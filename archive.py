#!/usr/bin/python3

import glob
import os
import time
from common import utc_8601_to_timestamp, sortable_time, print_to_log

space_name = 'subwaydataproject'

import boto3
from botocore.client import Config

# Start the log file
(year, month, day, hours, mins, secs) = sortable_time()
logfile = 'logs/4-archive.'+year+'-' + month + '-' + day + '-' + hours + '-' + mins + '-' + secs + '.log'
log = open(logfile, 'w')
print_to_log(log, 'New archiving session')


# Initialize a session using DigitalOcean Spaces.
session = boto3.session.Session()
client = session.client('s3',
                        region_name='nyc3',
                        endpoint_url='https://nyc3.digitaloceanspaces.com',
                        aws_access_key_id='KP5XVAQXHLD2HXFTGURL',
                        aws_secret_access_key='G/0ojMwrwlEFJIM1kSbJVrKM+4Ma6S/UiUPvkMUgYvI')
import hashlib
from functools import partial

def md5sum(filename):
    with open(filename, mode='rb') as f:
        d = hashlib.md5()
        for buf in iter(partial(f.read, 128), b''):
            d.update(buf)
    return d.hexdigest()

n_failed = 0
n_uploaded = 0

current_time = time.time()

files = glob.iglob('3-compressed/*/*/*.tar.bz2')    

for file_name in files:

    a = file_name.rfind('/')
    b = file_name.find('-',a)
    c = file_name.find('.',b)
    utc = file_name[b+1:c] + '0000Z'
    t = utc_8601_to_timestamp(utc)

    #If this file was created more than 2 minutes ago, upload it
    if current_time-t > 120:
        path = file_name[file_name.find('/')+1:]
        md5 = md5sum(file_name)
        print_to_log(log, 'Uploading: ' + file_name)
        print_to_log(log, '    md5: ' + md5)
        try:
            client.upload_file(file_name,'subwaydataproject','agg1/' + path, ExtraArgs = { "Metadata": {"md5chksum": md5} })

        except Exception as e:
            n_failed += 1
            print_to_log(log, 'Error in upload; Exception follows')
            print_to_log(log, str(e))
            print('Failed to upload ' + file_name)
            continue
    
        n_uploaded += 1
        os.remove(file_name)
        print_to_log(log, '    success; original file deleted')
        print('Uploaded ' + file_name)


        if n_uploaded + n_failed >= 100:
            print('Max number of uploads reached')
            print_to_log(log, 'Max number of uploads reached')
            break

#client.upload_file('1-download.py','subwaydataproject','test1/test2/1-download.py')

print_to_log(log, 'Successfully uploaded ' + str(n_uploaded) + ' files')
print_to_log(log, 'Failed to upload ' + str(n_failed) + ' files')


