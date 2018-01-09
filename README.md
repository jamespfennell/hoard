# Realtime Aggregator

## Introduction
Hundreds of transit authorities worldwide freely distribute realtime information about their trains, buses and other services
using a variety of formats, principally the General Transit Feed Specification (GTFS) format. 
This real time data is primarily designed to help their customers plan
their journeys in the moment, however when aggregated over a period of time it becomes an extensive data set that 
	can be used to answer relevant questions.
Such historical data could be used to evaluate past performance ('what percentage of trains arrived on time at Central Station between 5pm and 7pm?')
or improve the transit predictions themselves (presumably using techniques from data science).

This software was developed in order to aggregate such realtime data.
The software was designed with the following principles in mind:
* Be reliable: the aggregating software cutting out for 8 hours overnight is unacceptable as it would lead to 
	a patchy data set. The software is designed to be robust and to be deployed with redunancy. 
* Be space efficient: the flip side of redundancy is that significantly more data is downloaded than needed.
	If the New York City subway realtime data is downloaded every 5 seconds, 2 gigabytes of data is generated each day.
	The software removes duplicate data, compresses data by the hour, and offers the facility of transferring data from
	the local (expensive) server to remove (cheap) bucket storage.
* Be flexible: transit authorities don't just use GTFS: the New York City subway, for example, also distributes data in an ad hoc XML format.
	The software can handle any kind of file-based realtime feed.

## Getting Started

### Prerequisites

The software is written in Python 3. 

It requires a number of additional

boto3

google.transit



### Installing

### Deploying

CRON:


crontab schedules.cron

## How the software works

The purpose of this software to manage the aggregation of GTFS data. A first approach would be to simply have a script download the appropriate GTFS files
from the transit authority at some interval of time, say 15 seconds. The present software addresses some serious technical difficulties with this first approach:

* The disk space required would probably be enormous. For example, by simply downloading the GTFS feeds for the New York City subway every 15 seconds,
	one generates about two gigabytes of data per day. Some kind of compression is needed to avoid ballooning hard disk costs.

* The aggregating software *has* to be downloading data at all times. It would be unacceptable to have 4 hours of data missing because the script
	encountered an error during the night, terminated, and was only restarted in the morning when the issue was noticed by a admin.

These two considerations determine the overall design of the software. The first consideration means that the software will have multiple tasks to complete:
as well as downloading data, it will have to compress it. The software groups GTFS files from the same feed into hour blocks for compression purposes.
That is, all the GTFS files from 14:00:00 to 14:59:59 will be grouped and compressed together.

The software is partitioned into four modules, each of which complete one task in the chain and operates independently of the others.

Module 1: DOWNLOAD: Download the GTFS files from the transit authority website every N seconds and store them. This module is the most critical. Even if 
the subsequent modules fail, it is essential that at least the raw real time data is being stored. This module must be running all the time.

Module 2: FILTER: Having downloaded the GTFS files every N seconds, there may be some duplication, namely if the transit authority hasn't updated the
real time information in the intervening period. This module opens the GTFS files, reads their timestamps, and basically deletes duplicates.
This module also deletes corrupted GTFS files. By default, this module runs every 5 minutes.

Module 3: COMPRESS: The unique GTFS files from the previous step are compressed. The files corresponding to the same hour and same feed are grouped together
and placed in a tar.bz2 file. By default, this module runs every hour. (Because this module waits until all the GTFS files for a given hour are downloaded,
it is pointless to run it more frequently.)

Module 4: ARCHIVE: In general, disk space on a server is more expensive than on a storage facility like Digital Ocean Spaces or AWS Storage. The
last module transfers the compressed files from the local server to a storage facility. This way, it is possible run the aggregating software
on a cheap server with a small amount disk space in combination with a large storage facility. This module can easily be disabled if it is desired to
keep the compressed files locally. By default, this module runs every hour, 20 minutes after the previous module.












