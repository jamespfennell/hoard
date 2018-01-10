# Reading the log files

Because the Realtime Aggregator software is run largely from Cron, extensive log files are created so that the progress
and performance of the aggregation may be tracked.
Log files are, by default, stored in the `logs` subdirectory of the program files.
If a different working directory is specified by using the `directory` command line option,
	the log files will be stored in the `logs` subdirectory of that working directory.

Realtime Aggregator generates two kinds of log file:
individual log files for each run of each of the four tasks,
and master log files that contain information from all of the tasks in a given hour.
The master log files will usually be the first log files you inspect.

## Master log files

For every clock hour in which a task is initiated, there will be a master log file.
For the clock hour `hh` on day `mm/dd/yyyy`, this master log file will be found at
```
logs/master/master-yyyy-mm-ddThh.log
```
Every task initiated during a given clock hour will publish a little information to the master log.
Each task will record that it has started and state the parameters it is using,
and when it concludes it will record this and give some statistics on the run.
If the task fails because of an exception, this will be noted and the exception text will be recorded.
The master log is thus where you get the big picture: it is designed to be used
to verify that everything is working correctly, and if not it gives you indicators on which tasks are
having problems.


Many tasks, including most download tasks and many archive tasks, take minutes to run,
and this has consequences for navigating the master logs.
For the sake of simplicity, consider a situation when a download task of duration 10 minutes begins
at 2:55pm, and a filter task begins at 2:56pm.
At 2:55pm, the download task will record that it has begun.
At 2:56pm, the filter task will record in the same log that it has begun.
Because filter tasks generally run quickly,
	it will likely record that it has concluded within the same minute.
At 3:05pm, the download task will record in the same log that it has concluded.
**Note**: even though the download task concludes at 3:05pm, this fact is recorded in the log
 corresponding to the 2:00pm-2:59pm clock hour. Tasks always log to the master log file corresponding to the 
clock hour in which they were initiated. This ensures that output from the same task appears in the same log.

The example here illustrates a problem: multiple tasks are writing to the log in a shuffled way:
the download task first records before the filter task, and records again after it.
In order for the reader to tell which log messages correspond to which tasks, every task
is given an integer `task_id` that is generally unique within the clock hour.
Each message in the log is prefixed with this `task_id`, so the reader can identify messages from the same task.

## Individual log files for each task instance

Each of the four tasks creates a log file each time it is run.
For a task initiated at time `hh:MM:ss` on date `mm/dd/yyyy`, the log file will appear at
```
logs/<task name>/<task name>-yyyy-mm-ddThhMMss.log
```
where `<task name>` is one of `download`, `filter`, `compress` and `archive`.
Observe that the program assumes that tasks are initiated in different seconds.

Each log file contains extensive information on the task run.
The log file begins by stating the parameters and feed settings in use.
The subsequent information then depends on the particular task.

* **Download task**: for each download cycle, the task records the directory files are being downloaded to, and
	the number of successful and failed downloads.
	At the end the task records the total number of successful and failed downloads.

* **Filter task**: the filter task largely copies files from the `store/downloaded` subdirectory to the
	`store/filtered` directory. Each copy operation is recorded, including the source and target file names.
	The presence of duplicate files is recorded.
	For every corrupt file, some description is given as to why the program determined the file is corrupt.
	At the end the task records the total number of files that were copied, the total number of duplicates found
	and the total number of corrupt fles.

	For New York City subway data, about 2% of the downloaded files are deemed corrupt.

* **Compress task**: the compress task records every compression operation that takes place, including the directory
	of files being compressed and the target `.tar.bz2` file. 
	If the compressed file already exists, so that the new filtered files need to be merged into the existing
	compressed file, this will be recorded also.
	At the end the task records the total number of compressed files created.

* **Archive task**: for each upload, the archive task records the source file, the MD5 hash of the source file,
	and the target object storage key.
	If the object already exists in storage, this is noted and the merging process is logged.
	At the end the task records the number of succesful and failed file uploads.


## Testing log file

When test running the software through
```
$ python3 realtimeaggregator.py test
```
the master log is not used and individual log files for each action are not created.
Instead, information that would be recorded in the master log is printed to the standard output,
and messages for specific tasks are put together in `log/test.log`.

## Remove old log files

The program is distributed with a standalone `cleanlogs.py` script that deletes all log files that are more than 24 hours old.
You may choose to regularly schedule this script with Cron, or you may periodically check that the software is running correctly and
delete the logs after this check.
Note that if you used the `directory` command line option to use a different working directory for the aggregation, you
will need to provide this to the `cleanlogs.py` script also:
```
$ python3 cleanlogs.py <path to working directory>
```






