# Realtime Aggregator

## Introduction
Hundreds of transit authorities worldwide freely distribute realtime information about their trains, buses and other services
using a variety of formats, principally the General Transit Feed Specification (GTFS) format. 
This real time data is primarily designed to help their customers plan
their journeys in the moment, however when aggregated over a period of time it becomes an extensive data set that 
	can be used to answer interesting questions.
Such data could be used to evaluate past performance ('what percentage of trains arrived on time at Penn Station between 5pm and 7pm?')
or improve the transit authority's realtime predictions (using techniques from data science).

This software was developed in order to aggregate such realtime data.
The software was designed with the following principles in mind:
* **Be reliable**: the aggregating software need to generate a data set that is complete: cutting out for an hour is unacceptable.
	The software is designed to be robust and to be easily deployed with multiple layers of redundany.
 
* **Be space efficient**: the flip side of redundancy is that significantly more data is downloaded than needed.
	If the New York City subway realtime data is downloaded every 5 seconds, 2 gigabytes of data is generated each day!
	The software removes duplicate data, compresses data by the hour, and offers the facility of transferring data from
	the local (expensive) server to remote (cheap) object storage.

* **Be flexible**: transit authorities don't just use GTFS: the New York City subway itself also distributes data in an *ad hoc* XML format.
	The software can handle any kind of file-based realtime feed once the user provides a Python function for determining
	the feed's publication time from its content.
	The software has GTFS functionality built-in.

## Getting Started

### Prerequisites

The software is written in Python 3. 

A number of features require additional Python 3 packages. 
To use the built-in GTFS functionality, the `google.transit` package is required; this can be installed using Pip:
```
pip3 install boto3
```

The software can transfer compressed data files from the local server to a remote object storage server, for example
	[Amazon S3 storage](https://aws.amazon.com/s3/) or 
	[Digital Ocean spaces](https://www.digitalocean.com/products/object-storage/).
To use this functionality, the `boto3` package is required; this can be installed using Pip:
```
pip3 install boto3
```

You will additionally need to specify your object storage settings in the `remote_settings.py` file.
The required settings are described in detail in that file.




### Installing

Download the files to any directory.
The program needs to have permission to create and delete subdirectories and files within the directory it is installed.

Before running the program, you need to specify the realtime feeds you want to aggregate.
These feed settings are placed in `remote_settings.py`; detailed instructions are provided inside that file.

The software is employed through a command line interface.
The full interface can be explored by running:
```
$ python3 realtime-aggregator.py -h
```
To begin, you're going to want to test the software to ensure that it is running correctly and downloading your feeds correctly.
To do this, run:
```
$ python3 realtime-aggregator.py test
```
This command performs all the tasks involved in the aggregating process as described in the next section.
If you have specified remote object storage, `.tar.bz2` files containing the feeds will be uploaded to your remote storage.
Otherwise, the `.tar.bz2` files may be found and inspected in the `store/compressed/` subdirectory.



### Deploying

After using the test feature to verify that the software is working and that your settings are valid,
	you will want to deploy the software to begin the aggregation proper.
To aggregate 24/7, the software should naturally be running on a server that is always on!

The aggregation process involves three *tasks*, with an optional fourth task. # that uploads compressed files to remote object storage.
In order to aggregate, these tasks need to be scheduled regularly by Cron or a similar facility.
The tasks are:

1. **Download task**.
	This is the task that actually downloads the feeds to the local server.
	It is the only task that runs continuously.
	It downloads the feeds at a certain frequency (by default every 14 seconds) and concludes after a certain amount of time (by default every 15 minutes).
	Your system should be set up so that when a download task concludes, Cron starts a new download task to keep the downloading going.

	The download task is the most critical component of the software.
	To create a complete data set, it is essential that the feeds are always being downloaded.
	In deployments, one should consider scheduling download tasks with redunancy.
	For example, one could schedule a download task of duration 15 minutes to start every 5 minutes.
	That way, at a given time three download tasks will be running simultanoulsy and so up to two can fail without any data loss.

2. **Filter task**.
	This task filters the downloads by removing duplicates and corrupt files.
	It can be run as frequently as one wishes: by default it runs every 5 minutes.

3. **Compress task**.
	This task compresses the filtered feed downloads for a given clock hour into one `.tar.bz2` archive for each feed.
	The compress task only compresses a given clock hour when the program knows that all the downloads for that clock hour have been filtered.
	(However, if more downloads for a given clock hour subsequently appear, the compress task will add these to the relevant archive.)
	Because the compress task compressess by the clock hour, it is only necessary to run it at most once an hour.

4. **Archive task**.
	This task trasfers the compressed archives from the local server to remote object storage.
	This is esentially a money-saving operation, as bucket storage is about 10% the cost of server space per gigabyte.


The `schedules.crontab` file instructs Cron to schedule tasks as described here.
This file needs to be installed for Cron to work from it:
```
crontab schedules.crontab
```


### Two notes on consistent aggregation

1. As mentioned before, it is essential that the software be downloading feeds all the time.
Redundancy may be introduced by scheduling multiple, overlapping download tasks.
One can introduce further redundancy by scheduling multiple autonomous aggregator sessions using the Cron file.
Such sessions would track the same feeds, but download to different directories locally, and then, when uploading to remote storage,
	use different object keys to store the output simultaneously.
See the Advanced Usage guide in under `docs/`

2. You will be running the software on a server, but sometimes it may be necessary to restart the server or otherwise pause the aggregation on that box.
In this case, one can run the aggregation software with the same object storage settings on a different device. 
The software is designed so that the compressed archive files from two different instances being uploaded to the same location in the object storage 
	will be merged.
Again, see the Advanced Usage guide.




## What next?


The `docs/` folder contains further information on the software: how it works, and how you my get more out of it.
You can also learn about the logs that are created, for checking that the aggregation is working.








