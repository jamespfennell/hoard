=====================
Reading the log files
=====================

Because the Realtime Aggregator software is usually run from Cron, 
extensive log files are created so that the progress
and performance of the aggregation may be tracked.

Log files are, by default, stored in the ``logs`` subdirectory of
the working directory in which the program is run.
If a different working directory is specified by using the ``directory``
command line option of ``realtimeaggregator``,
the log files will be stored in the ``logs`` subdirectory of that working
directory.
Within the ``logs`` directory, logs are placed in a sub directory 
corresponding
to the current hour: ``yyyy-mm-dd/hh/download-.log``.
Each task logs to a distinct log file.

Starting with version 0.2.0, Realtime Aggregator is logging using 
protocol buffers.
With this framework logs are not generated in a human readable format.
Instead data about the run is stored in a semi-structured format
that is then serialized to binary and stored as binary on disk.
This approach is beneficial as logs can be automatically read, and 
human readable hourly and daily digests created.
However the migration to protocol buffers is not complete,
and right now all that is available is a crude
log reading mechanism.


If you installed the Realtime Aggregator through Pip3, a
``realtimeaggregatorlogs`` executable will be available to you.
This executable is run through the command line; available options may
be seen by running::

    $ realtimeaggregatorlogs -h

As of now, the only possible action is to read a specific log file::

    $ realtimeaggregatorlogs read path/to/log_file/download-2018-03-03T124532Z.log

This command will output a plaintext version of the
log. It is not very human friendly right now, but still good enough
to see what is happening.

A much more extensive facility for reading logs and automatically
creating human readable digests is planned for the version 0.3.0 release.


