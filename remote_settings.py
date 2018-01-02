"""
This file contains information about the realtime feeds that you, the user, wishes to aggregate.
It also contains information about the bucket storage service you with to use, if relevant.


(1) FEED INFORMATION

Each feed to be aggregated appears as one entry in the feeds list
The entry in the feeds list corresponding to a feed is a 3 tuple [(a), (b), (c)] with the following entries:
   (a) String: a unique identifier (within the aggregating software) for the feed. For example, if tracking a subway line the uid may be the line's name.
   (b) String: the URL where the feed is downloaded from.
   (c) Function reference: reference to a function that determines if a given feed download is valid and, if so, returns the timestamp of the download.

More precisely for (c), the function referred to has the following format:

def timestamp_from_feedtype(file_path):
   Given a file located locally at file_path, determine if the file is formatted correctly, and if so determine the time is was published by the transit authority.
   If the file is invalid, return -1
   If the file is valid, return its Unix timestamp 

A function that does this for the GTFS realtime is given below, and may be instructive to inspect if writing a custom function
(for example if a feed you're interested in is published in a custom XML format).
"""



def timestamp_from_gtfs_realtime(file_path):
    """Check if the file given locally by file_path is a valid GTFS realtime file, and if so return its Unix timestamp
    If the file is invalid, return -1.

    Keyword arguments:
    file_path -- path to a file on the local server.
    """

    # Open the file in binary format, and read into a string
    f = open(file_path, 'rb')
    data = f.read()
    f.close()

    # Try to interpret it as a GTFS realtime file
    # If there is an error reading it, the file is corrupt or otherwise not a valid GTFS realtime file
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(data)
    except:
        return -1

    # Otherwise, read the time stamp on the GTFS file
    # If the time stamp is 0, the file is invalid
    # Otherwise, the file is valid so return its timestamp
    feed_time = feed.header.timestamp
    if feed_time == 0:
        return -1
    else:
        return feed_time


# Feed information proper

mta_api_key = '18f496b0ab3ae1fbb1aa471e63377ce4'


feeds = [
        ['12356', 'http://datamine.mta.info/mta_esi.php?key=' + mta_api_key + '&feed_id=1', 'gtfs', timestamp_from_gtfs_realtime],
        ['ACE', 'http://datamine.mta.info/mta_esi.php?key=' + mta_api_key + '&feed_id=26', 'gtfs', timestamp_from_gtfs_realtime],
        ['BDFM', 'http://datamine.mta.info/mta_esi.php?key=' + mta_api_key + '&feed_id=21', 'gtfs', timestamp_from_gtfs_realtime],
        ['G', 'http://datamine.mta.info/mta_esi.php?key=' + mta_api_key + '&feed_id=31', 'gtfs', timestamp_from_gtfs_realtime],
        ['JZ', 'http://datamine.mta.info/mta_esi.php?key=' + mta_api_key + '&feed_id=36', 'gtfs', timestamp_from_gtfs_realtime],
        ['L', 'http://datamine.mta.info/mta_esi.php?key=' + mta_api_key + '&feed_id=2', 'gtfs', timestamp_from_gtfs_realtime],
        ['NQRW', 'http://datamine.mta.info/mta_esi.php?key=' + mta_api_key + '&feed_id=16', 'gtfs', timestamp_from_gtfs_realtime],
        ['SIR', 'http://datamine.mta.info/mta_esi.php?key=' + mta_api_key + '&feed_id=11', 'gtfs', timestamp_from_gtfs_realtime],
        ['TEST', 'https://www.realtimerail.nyc/', 'txt', timestamp_from_gtfs_realtime]
        ]


"""
(2) BUCKET STORAGE SETTINGS
"""

using_remote_storage = False


