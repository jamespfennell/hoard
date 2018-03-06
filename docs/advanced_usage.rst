====================
Advanced usage guide
====================

-----------------------------------------
Using a different local storage directory
-----------------------------------------

By default, the program
stores downloaded feeds and the logs in 
the current working directory.
It places
downloaded feeds in a ``feeds`` subdirectory and logs in a ``logs``
subdirectory.

The storage location can be 
changed using the ``-r`` option at the command line; for example::

    $ realtimeaggregator -r ~/aggregator/ download

------------------------------------------------------------
Running multiple aggregation instances from one installation
------------------------------------------------------------


Using the storage directory option ``-r``
it is possible to run multiple aggregation instances from the same installation.
You may want to do this to provide another layer of redundancy,
or to run autonomous aggregation instances for different feeds.

^^^^^^^^^^
Same feeds
^^^^^^^^^^

To run two aggregation instances for the same feeds,
you could place the following commands in the ``crontab`` file::

    */5    * * * * realtimeaggregator -r session1/ download
    1-59/5 * * * * realtimeaggregator -r session1/ filter
    22     * * * * realtimeaggregator -r session1/ compress

    */5    * * * * realtimeaggregator -r session2/ download
    1-59/5 * * * * realtimeaggregator -r session2/ filter
    22     * * * * realtimeaggregator -r session2/ compress

This will download, filter, and compress
the same feeds in two separate local directories.

Note that both of these instances are using the same remote settings file.
If bucket storage is activated, and an archive task is performed,
both instances will upload their files to the same
location.
This will provide some redundancy: the sets of feeds will be merged,
so one instance missing a data point won't be problematical.
However one can also have the two instances upload the files
to different keys in object storage using the local prefix ``-p`` option::

    43 * * * * realtimeaggregator -r session1/ -p session1/ archive
    43 * * * * realtimeaggregator -r session2/ -p session2/ archive

This will prefix the object storage keys by ``session1/`` and
``session2/`` respectively.
The flip side of the increased redundancy here
is that now you will be storing the (potentially large) data set twice.

^^^^^^^^^^^^^^^
Different feeds
^^^^^^^^^^^^^^^

All of the examples above used the same default remote settings file
``remote_settings.py``. If you wish to have two instances aggregating
different feeds, you need two remote settings files containing
the feed information for each instance respectively.
Then use the `-s` option to use the non-default remote settings files::

    */5 * * * * realtimeaggregator -r session1/ -s remote_settings1.py download
    #etc

    */5 * * * * realtimeaggregator -r session2/ -s remote_settings2.py download
    #etc

In this case you can safely run archive tasks without file
name collisions in object storage by setting different global
prefixes in the respective remote settings files.

--------------------------------------------------------------
Running concurrent aggregation instances on different machines
--------------------------------------------------------------

You will likely deploy the aggregator on a remote server that is active 24/7.
However it may sometimes be necessary to stop the server (for example,
to perform security updates).
A strategy is needed to keep the aggregation running during such server
downtime.

The solution is to run distinct aggregation instances on separate 
machines using the same remote object storage settings. 
It is not necessary to use another server. If your remote
server will be down at a scheduled time, you can simply run the 
aggregation software on your personal computer during the downtime.

The aggregation instances will download, filter, and compress separately
and then, during archive tasks, 
both attempt to upload to the same object storage location.
However if an archive task finds a preexisting ``.tar.bz2``
file at the object storage 
location it is planning to upload to, it does not overwrite it.
It downloads the ``.tar.bz2`` file, merges it with the local compressed 
``.tar.bz2`` file, 
and then uploads the complete file to the object storage.
The feeds from the separate instances are thus merged.

The key to getting this right is to ensure that the two
concurrent aggregation instances don't attempt to upload to 
the object storage at the same time.
This can be guaranteed by appropriate settings in Cron, for example::

    # Remote server
    43 * * * * realtimeaggregator -s same_remote_settings.py archive

    # Backup server/local computer
    53 * * * * realtimeaggregator -s same_remote_settings.py archive


