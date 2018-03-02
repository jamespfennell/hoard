"""Provides a number of functions related to time."""

import time
import calendar

def timestamp_to_path_pieces(timestamp=-1):
    
    t =  timestamp_to_data_list(timestamp=-1)
    (year, month, day, hour, _, _) = t
    day_dir = '{}-{}-{}'.format(year, month, day)
    hour_sub_dir = hour
    file_time = timestamp_to_utc_8601(timestamp)
    return (day_dir, hour_sub_dir, file_time)

    

def timestamp_to_utc_8601(timestamp=-1):
    """Given a unix timestamp, return the UTC 8601 time string in the form
    YYYY-MM-DDTHHMMSSZ, where T and Z are constants and the remaining letters
    are substituted by the associated date time elements.

    For example, timestamp_to_utc_8601(1515174235) returns
    '2018-01-05T174355Z', which corresponds to the time 17:43:55 on
    January 5th, 2018 (UTC time).

    Arguments:
        timestamp (int): an integer representing the time as a Unix timestamp.
                         If -1, set equal to the current Unix time.
    """
    t = timestamp_to_data_list(timestamp)
    return '{}-{}-{}T{}{}{}Z'.format(*t)


def utc_8601_to_timestamp(utc):
    """Given a UTC 8601 time string in the form YYYY-MM-DDTHHMMSSZ,
    return the associated Unix timestamp.

    Arguments:
        utc (str): a UTC 8601 formatted string in the form YYYY-MM-DDTHHMMSSZ
                   where T and Z are constants and the remaining letters
                   are substituted by the associated datetime elements.
    """
    # Read the datetime elements from the string
    year = int(utc[0:4])
    month = int(utc[5:7])
    day = int(utc[8:10])
    hour = int(utc[11:13])
    mins = int(utc[13:15])
    secs = int(utc[15:17])
    # Put the elements in the from of a time struct
    t = (year, month, day, hour, mins, secs, -1, -1, 0)
    # Use calendar to convert the time struct into a Unix timestamp
    return calendar.timegm(t)


def timestamp_to_data_list(timestamp=-1):
    """Return a 6-tuple of strings (year, month, day, hour, minute, second)
    representing the time given by the Unix timestamp. The year string will
    have length exactly 4 and the other strings will have length exactly 2,
    with left 0 padding if necessary to achieve this.

    Arguments:
        timestamp (int): a integer representing the time as a Unix timestamp.
                         If -1, set equal to the current Unix time.
    """
    if timestamp == -1:
        now = time.gmtime()
    else:
        now = time.gmtime(timestamp)
    # Read the data from the time struct.
    soln = [str(now.tm_year),
            str(now.tm_mon),
            str(now.tm_mday),
            str(now.tm_hour),
            str(now.tm_min),
            str(now.tm_sec)
            ]
    # Left pad with zeroes if necessary, and return.
    for k in range(1, 6):
        if len(soln[k]) == 1:
            soln[k] = '0' + soln[k]
    return soln
