=========================
Realtime Aggregator 0.2.0
=========================

------------
Introduction
------------

Hundreds of transit authorities worldwide freely distribute realtime
information about their trains, buses and other services
using a variety of formats, principally the
General Transit Feed Specification (GTFS) format. 
This realtime data is primarily designed to help their customers plan
their journeys in the moment, however when aggregated over a period of
time it becomes an extensive data set that 
can be used to answer interesting questions.
Such data could be used to evaluate past performance ('what percentage
of trains arrived on time at Penn Station between 5pm and 7pm?')
or possibly to improve the transit authority's realtime predictions.

This software was developed in order to aggregate such realtime data.
The software was designed with the following principles in mind:

* **Be reliable**: the program is tasked with generating a *complete* data
  set: the aggregation process
  cutting out for an hour is unacceptable.
  The software is designed to be robust and to be easily deployed
  with multiple layers of redundancy.
 
* **Be space efficient**: the flip side of redundancy is that significantly
  more data is downloaded and processed than needed.
  If the New York City subway realtime data was downloaded every 5
  seconds, 2 gigabytes of data would be generated each day!
  The software removes duplicate and corrupt
  data, compresses data by the hour,
  and offers the facility of transferring data from
  the local (expensive) server to remote (cheap) object storage.

* **Be flexible**: transit authorities don't just use GTFS: the New York
  City transit authority, for example,
  also distributes data in an ad hoc XML format.
  The software can handle any kind of file-based realtime feed once 
  the user provides a Python function for determining
  the feed's publication time from its content.
  The software has GTFS functionality built-in.

---------------
Getting Started
---------------

^^^^^^^^^^^^
Installation
^^^^^^^^^^^^

Realtime Aggregator requires Python 3.5 or above.
The easiest way to install it is through Pip::

    $ pip3 install realtimeaggregator

There are three package dependencies which Pip will install automatically:
``requests`` (for downloading feeds),
``gtfs-realtime-bindings`` (for the built-in GTFS functionality), and
``boto3`` (for uploading files to remote object storage).
Given your particular situation you may want to install the software
in a virtual environment.

^^^^^^^^^^
Setting up
^^^^^^^^^^

In order to begin aggregating you need to specify remote settings.
These describe where the the feeds you want to aggregate are to 
be downloaded,
the gives details about the object storage where the aggregated
feeds should be uploaded to (if using object storage).
Remote settings are specified by you in a special Python file: to generate
a template for this file run::

    $ realtimeaggregatr makersf

(The command ``makersf`` should be read as
*make* *r*\ emote *s*\ ettings *f*\ ile.)
This command will place a file
``remote_settings.py`` in your working directory.
Details of how to customize your settings are given in that file.
For each feed you wish to aggregate you need to specifiy four items:

#. A unique identifier ``uid`` for the feed. This is merely used for the 
   program to internally distinguish the different feeds you are
   aggregating, and is completely up to you. 

#. The URL where the feed is to be downloaded from.

#. A file extension for the feed, for example ``gtfs``, ``txt``, or ``xml``.

#. A Python 3 function that, given the location of a feed download locally,
   determines if it is a valid feed (for example,
   was not corrupted during the download process) and, if so, returns
   the time at which the feed was published by the transit authority.
   Such a function for GTFS Realtime is provided in the default
   ``remote_settings.py`` file.

For convenience, the program is distributed with the feed settings for two
New York City subway lines,
although you will need an 
`API key <http://datamine.mta.info/>` from the 
Metropolitan Transit Authority 
to use them.

If you wish to use remote object storage, relevant settings also need to
be set in ``remote_settings.py``; details are inside the file.


^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Testing your remote settings
^^^^^^^^^^^^^^^^^^^^^^^^^^^^


The program is invoked through a command line interface.
The full interface can be explored by running::

    $ realtimeaggregator -h

To begin, you're going to want to test the software to ensure that it is
running properly and that your remote settings are correct.
To do this, run::

    $ realtimeaggregator testrun

This command performs all the tasks involved in the aggregating process,
as described below.
If you have specified remote object storage, ``.tar.bz2`` files containing the
feeds will be uploaded to your remote storage.
You should check your remote storage to ensure the files were uploaded 
successfully. 
By default, the object key for the aggregated files for feed ``uid`` aggregated 
in clock hour ``hh`` on the date ``mm/dd/yyyy`` will be at::

    realtime-aggregator/yyyy-mm-dd/hh/uid-yyyy-mm-ddThh0000Z.tar.bz2

Otherwise, the ``.tar.bz2`` files may be found and inspected locally in the 
``feeds/compressed/`` subdirectory of your current working directory.
The aggregated files for feed ``uid`` aggregated in clock hour ``hh`` on the
date ``mm/dd/yyyy`` will be at::

    feeds/compressed/yyyy-mm-dd/hh/uid-yyyy-mm-ddThh0000Z.tar.bz2

The string ``yyyy-mm-ddThh0000Z`` appearing in the file name is
the `ISO 8601 <https://en.wikipedia.org/wiki/ISO_8601>`
representation of the clock hour.
As you can see, by default data for different days and hours is placed
in different directories,
however the file naming scheme is designed so that all the data you
aggregate can subsequently be placed together in a single directory.

**Note that all times are in UTC!**
This ensures that aggregating instances for the same feeds can be run
on machines with different local times, and also avoids the usual
headaches with timezones.

----------
Deployment
----------


After using the test feature to verify that the software is working
and that your settings are valid,
you will want to deploy the software to begin the aggregation
proper.
To aggregate 24/7, the software should be running on a server that is always on!

The aggregation process involves three *tasks*, with an optional fourth task.
In order to aggregate, these tasks need to be scheduled regularly by Cron
or a similar facility.
The tasks are:

#. **Download task**.
   This is the task that actually downloads the feeds from the 
   transit authority's server to your local server.
   It is the only task that runs continuously.
   It downloads the feeds at a certain frequency (by default every
   14 seconds) and concludes after a certain amount of time 
   (by default after 15 minutes).
   Your system should be set up so that when a download task concludes,
   Cron starts a new download task to keep the download process going.

   The download task is the most critical component of the software.
   To create a complete data set, it is essential that there is at
   least one download task running at all times.
   In deployments, one should consider scheduling download tasks 
   with redundancy.
   For example, one could schedule a download task of duration 15	
   minutes to start every 5 minutes.
   That way, at a given time three download tasks will be running	 
   simultaneously and so up to two can fail without any data loss.

#. **Filter task**.
   This task filters the files that have been downloaded by
   removing duplicates and corrupt files.
   It can be run as frequently as one wishes: by default 
   it runs every 5 minutes.

#. **Compress task**.
   This task compresses the filtered feed downloads for a given clock
   hour into one ``.tar.bz2`` archive for each feed.
   The compress task only compresses a given clock hour when the program
   knows that all the downloads for that clock hour have been filtered.
   (However, if more downloads for a given clock hour subsequently
   appear, the compress task will add these to the relevant archive.)
   Because the compress task compressess by the clock hour, it 
   need only be scheduled once an hour.

#. **Archive task**.
   This task trasfers the compressed archives from the local server
   to remote object storage.
   This is esentially a money-saving operation, as bucket storage is
   about 10% the cost of server space per gigabyte.

The software comes with a default
``schedules.crontab`` file for scheduling these
tasks. 
To place a copy of this file in your current working directory run::

    $ realtimeaggregator makectf
    
(The command ``makectf`` should be read as
*make* *c*\ ron\ *t*\ ab *f*\ ile.)
The ``schedules.crontab`` file contains the default Cron settings
and instructions for changing them.
This file needs to be installed with Cron::

    $ crontab schedules.crontab

Remember that usually each user only gets one crontab file.
If you
have another crontab file in use, you will need to merge the 
two files together before invoking ``crontab``.

Once the Cron file has been installed, the aggregation will begin in the
background.
To ensure the aggregation is running successfully, you should check
your object storage or local server to see
that the relevant `.tar.bz2` files are appearing and that they contain
the correct feeds and at the right frequency.
Note that after you install the Cron file, it will take at least an hour
for these archives to appear.
You should also consult the log files, which describe how successful the
program is in terms of number of files downloaded,
number of compressed archives created, etc. 
The `reading the logs guide <docs/reading_the_logs.md>` describes how you
can navigate the log files.



^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Two notes on consistent aggregation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As mentioned before, it is essential that the software be downloading 
feeds all the time.
Redundancy may be introduced by scheduling multiple, overlapping download
tasks.
One can introduce further redundancy by scheduling multiple, autonomous
aggregator sessions using the Cron file.
Such sessions would track the same feeds, but download to different 
directories locally, and then, when uploading to remote storage,
use different object keys to store the output simultaneously.
See the `advanced usage guide <docs/advanced_usage.md>`.

You will be running the software on a server, but sometimes it may be
necessary to restart the server or otherwise pause the aggregation
on that box.
In this case, one can run the aggregation software with the same object
storage settings on a different device. 
The software is designed so that the compressed archive files from two
different instances of the program
being uploaded to the same location in the object storage 
will be merged (rather than one upload overwritting the other).
However this is a little bit delicate to get right in practice; see
the `advanced usage guide <docs/advanced_usage.md>`.




----------
What next?
----------


The ``docs`` directory contains further documentation that may be of interest.

* The `reading the logs guide <docs/reading_the_logs.rst>` describes how
  you may navigate the log files
  to ensure the aggregation is operating succesfully.

* The `advanced usage guide <docs/advanced_usage.rst>` gives instructions 
  on going beyond the basic aggregation
  discussed here.









