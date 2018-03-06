"""
This file is where you, the user, specify which which feeds you wish
to aggregate. It also contains information about the bucket storage
service you wish to use, if relevant.

(1) FEED INFORMATION

Each feed to be aggregated appears as one entry in the feeds list.
The entry in the feeds list corresponding to a feed is a 4 tuple
[(a), (b), (c), (d)] with the following entries:
   (a) String: a unique identifier (within the aggregating software) for
       the feed. For example, if tracking a subway line the uid may be the
       line's name.
   (b) String: the URL where the feed is downloaded from.
   (c) String: the file extension, for example 'txt' or 'gtfs'
   (d) Function reference: reference to a function that determines if a
       given feed download is valid and, if so, returns the timestamp of
       the download.

More precisely for (d), the function referred to has the following format:

def timestamp_from_feedtype(file_path):
   Given a file located locally at file_path, determine if the file is
   formatted correctly, and if so determine the time is was published
   by the transit authority.
   If the file is invalid, return -1
   If the file is valid, return its Unix timestamp

Note that if any exception is raised during execution of the function, the
file is assumed to be invalid and the exception text is written to the filter
log. Therefore it is actually better to signify a file is invalid through a
raised exception (as opposed to returning -1) as then by reading the log
files one can discover why the file is invalid.

An implementation of this function for the GTFS realtime standard is given
below, and may be instructive to inspect if writing a custom function
(for example if a feed you're interested in is published in a custom XML
format).
"""

from google.transit import gtfs_realtime_pb2
import os
import datetime
import time


def timestamp_from_gtfs_realtime(file_path):
    """Check if the file given locally by file_path is a valid GTFS
    realtime file, and if so return its Unix timestamp
    If the file is invalid, return -1.

    Arguments:
        file_path (str): path to a file on the local server.
    """
    # Open the file in binary format, and read into a string
    f = open(file_path, 'rb')
    data = f.read()
    f.close()

    # Try to interpret it as a GTFS realtime file
    # If there is an error reading it, the file is corrupt or otherwise
    # not a valid GTFS realtime file
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)

    # Otherwise, read the time stamp on the GTFS file
    # If the time stamp is 0, the file is invalid
    # Otherwise, the file is valid so return its timestamp
    feed_time = feed.header.timestamp
    if feed_time == 0:
        return -1
    else:
        return feed_time


# Feed information proper

mta_api_key = '<get from mta.info>'
mta_url = ('http://datamine.mta.info/mta_esi.php?key=' +
           mta_api_key +
           '&feed_id=')

feeds = [
        ['12356', mta_url + '1', 'gtfs', timestamp_from_gtfs_realtime],
        ['ACE', mta_url + '26', 'gtfs', timestamp_from_gtfs_realtime],
        ]

"""
(2) BUCKET STORAGE SETTINGS

The archive action of the software tries moves compressed files to remote
bucket storage, if using_remote_storage is True. Bucket storage is provided,
for example, by Amazon Web Services (AWS) S3 Buckets or Digital Ocean Spaces,
and in general is an order of magnitude cheaper than hard disk space on a
server.

To use remote storage you need to insert some relevant settings here. In
general, the software uses the Python 3 boto3 module to connect to the
remote storage, and the settings you provide are basically handed over
directly to boto3. The first variable is boto3_client_settings, and this
is passed directly to boto3.session.Session().client(). The second variable
bucket is simple the name of your bucket.

The example below is for Digital Ocean Spaces.

You may optionally (and should probably) specify a global prefix. This will
be prefixed to the key of every file uploaded to your bucket, and maybe used
to distinguish uploaded files from other files in your bucket. At run time
you can also specify a local_prefix through the command line interface,
the full key will then be global_prefix + local_prefix + key. In this way
you can have multiple instances of the aggregator running and uploading to
different "parts" of your bucket, for redundancy.
"""

using_remote_storage = True

boto3_client_settings = {
        'service_name': 's3',
        'region_name': 'nyc3',
        'endpoint_url': 'https://nyc3.digitaloceanspaces.com',
        'aws_access_key_id': '<Get from Digital Ocean control panel>',
        'aws_secret_access_key': '<Get from Digital Ocean control panel>'}

bucket = 'bucket_name'

using_remote_storage = True

global_prefix = 'realtime-aggregator/'
