## Short term

1. Write about the structure of the program for developers in `docs/developers_guide.md`.
1. Write the advanced usage guide `docs/advanced_usage.md`.
1. Write comments in `remote_settings.py`.
1. The additional Python packages are optional, but are still imported by default. Fix this!

## Long term

1. Should there be more try/except blocks? The driver captures and records all non-system exceptions so possibly not? The overall
	exceptions philosphy is: *don't try to recover from
	errors in general because every task will be run again soon, instead ensure that errors don't cause a task to crash completely.
1. Make the internal documentation (i.e., the documentation in the `.py` files) more consistent.
1. Make internal variable naming consistent: in particular: `file_path`, `file_dir`, `file_name`.






