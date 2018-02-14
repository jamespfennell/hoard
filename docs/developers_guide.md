# Developer's guide

This is a brief guide to the code structure.
The program is not very big, so it's not too complicated.


The file layout:
```
realtimeaggregator.py
remote_settings.py
tasks/
    __init__.py
    archive.py
    compress.py
    download.py
    filter.py
    common/
        exceptions.py
        settings.py
        task.py
    tools/
        __init__.py
        filesys.py
        latesttimetracker.py
        logs.py
        time.py
```

The `realtimeaggregator.py` is a driver script that is
invoked through the command line. 
It is how the software is used.
It employs the Python package `argsparse` to read 
command line arguments, so the full interface can be seen by
running `python3 realtimeaggregator.py -h`.

The driver:

* Reads in the command line arguments and determines what to do,
* Imports the remote settings file and ensures that it's valid,
* Writes to the general log file, and then
* Performs the appropriate tasks based on the command line instructions.

The four distinct tasks are each associated to one module in
the `tasks` package. Each task takes the form of an object
(`DownloadTask`, `FilterTask`, etc) each of whose classes is derived from
the `Task` class. The `Task` class is defined in `tasks/common/task.py`.
The purpose of this structure is for each task to have the same interface,
which makes it easy for the driver to run different tasks.
In all cases, the driver performing a task means:

* Initializing the task: passing remote settings and log writing data,
	through the `Task.__init__()` method,
* Further initialization: depending on the task, further internal
	variables are set (for example, in the archive task remote
	object storage settings are passed in),
* Running the task using the `Task.run()` class method. 

Each task then does its own thing.


The `tasks/common` directory contains modules used by all
the tasks: the `Task` template, internal settings principally describing the
directory structure of local storage, and exceptions.

The `tasks/tools` package is a collection of disparate tools 
that are used by the software. These tools have no special relationship
to the software; they do generic things.

* `filesys.py` provides some methods for working with the filesystem.
* `latesttimetracker.py` provides a time tracking mechanism which allows
	independent Python processes (in this case, different task instances)
	to know the last time a specific event occured (in this case,
	the last download time of feed downloads that have been filtered).
* `logs.py` is a barebones logging module.
* `time.py` provides some methods for working with time, principally
	functions for translating between Unix timestamps and 
	ISO 8601 strings.

