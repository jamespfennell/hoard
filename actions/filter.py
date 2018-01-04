import calendar, glob, os, time
import shutil
from . import settings
from . import action
from . import tools

class FilterAction(action.Action):

    def run(self):
        """Run a filter action."""
        self.log_and_output('Running filter action.')

        # Initialize some necessary variables
        feeds_by_uid = {}
        for feed in self.feeds:
            feeds_by_uid[feed[0]] = feed
        self.n_total = 0
        self.n_corrupt = 0
        self.n_copied = 0
        self.n_skipped = 0
        last_download_time = -1
        limit_reached = False

        # Walk over the directory containing the raw downloaded files, inspecting every file
        # We attempt to process files that are feed downloaded, and remove other ('zombie') files
        for subdir, dirs, files in os.walk(self.root_dir + settings.downloaded_dir):
            for file_name in files:
                file_path = os.path.join(subdir, file_name)
                # Attempt to interpret the file as a feed download, whose filename has the form [UID]-YYYY-MM-DDTHHMMSSZ-dt.[EXT]
                # First, extract the UID from the file name and see if such a UID exists
                # Second, ensure the file extension matches that of the UID.
                # Finally, check the UTC string is valid (and if so store it: this is the download time)
                # If it's not a valid file, delete it
                try:
                    i = file_name.find('-')
                    uid = file_name[:i]
                    utc = file_name[i+1:i+19]
                    ext = file_name[i+23:]  
                    if uid not in feeds_by_uid or ext != feeds_by_uid[uid][2]:
                        raise Exception('UID and/or EXT does not match feeds.')
                    downloaded_timestamp = tools.time.utc_8601_to_timestamp(utc)
                    if downloaded_timestamp > last_download_time:
                        last_download_time = downloaded_timestamp
                except Exception as e:
                    os.remove(file_path)
                    self.log.write('Deleted zombie file ' + file_path)
                    self.log.write(str(e))
                    continue

                # Check the last modification time; if it was within file_access_lag seconds ignore this file to avoid file I/O clash with a download action
                if(time.time()-os.path.getmtime(file_path) < self.file_access_lag):
                    continue

                # This file will be processed, so increment the n_total counter
                self.n_total += 1

                # The next step is to try to read the feed timestamp: the time the transit authority distributed the feed.
                # In general this will be different to the download time because we may have downloaded a few seconds late.
                # The timestamp is read using the user-provided function.
                # If there is an exception raised, or if the timestamp is negative, mark the file as corrupt and delete it.
                try:
                    timestamp = feeds_by_uid[uid][3](file_path)
                    if timestamp < 0:
                        raise Exception('File timestamp is negative.')
                except Exception as e:
                    self.log.write('Corrupt file: ' + file_path)
                    self.log.write(str(e))
                    self.n_corrupt += 1
                    os.remove(file_path)
                    continue

                # The file is valid, we now move it over the filtered directory.
                # First we calculate the target directory and target filename within the filtered store using the timestamp
                t = tools.time.timestamp_to_data_list(timestamp)
                target_dir = self.root_dir + settings.filtered_dir + t[0] + '-' + t[1] + '-' + t[2] + '/' + t[3] + '/' + uid + '/'
                target_file_name = uid + '-' + tools.time.timestamp_to_utc_8601(timestamp) + '.' + ext
                tools.filesys.ensure_dir(target_dir)
                
                # If the target file does not exist, copy it
                # Otherwise this is a duplicate feed and can be skipped
                if not os.path.isfile(target_dir + target_file_name):
                    self.log.write('Copying: ' + file_path)
                    self.log.write('    to ' + target_dir + target_file_name)
                    shutil.copyfile(file_path, target_dir + target_file_name)
                    self.n_copied += 1
                else:
                    self.log.write('Skipping (duplicate):' + file_path + '.')
                    self.n_skipped += 1

                # Remove the original downloaded file
                os.remove(file_path)

                # Check if the limit of number of files to be processed has been reached
                if self.limit >= 0 and self.n_total >= self.limit:
                    limit_reached = True
                    self.log_and_output('Copy threshold reached; closing.')
                    self.output('Run again to filter more files.')
                    break

            # We're in a double for loop here, so need to break out of the second one too
            if limit_reached == True:
                break

        # At this stage the filtering process has ended, and we need to do some cleaning up.
        # First, by moving downloaded files over we may have a lot of empty subdirectories in the downloaded store, delete these.
        total = tools.filesys.prune_directory_tree(self.root_dir + settings.downloaded_dir)
        self.log.write('Removed ' + str(total) + ' empty directories in the downloaded store.')

        # We need to record the last downloaded time; this is needed to determine when all downloads for a given clock hour
        # are done and can be compressed.
        time_tracker = tools.latesttimetracker.LatestTimeTracker(settings.downloaded_time_dir)
        latest_time = time_tracker.add_time(last_download_time)

        # If the limit was not reached, so that all downloads were filtered, we may have to schedule compressions
        if limit_reached is False:
            # Iterate over all the filtered directories
            # If the corresponding hour is before the timestamp, then schedule for compression.
            # The scheduling is done by setting the compress flag, which is an empty file in the hour's directory entitled 'compress'
            files = glob.glob(self.root_dir + settings.filtered_dir + '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/[0-9][0-9]')    
            for file_path in files:
                hour = file_path[-2:]
                date = file_path[-13:-3]
                utc = date + 'T' + hour + '5959Z'
                t = tools.time.utc_8601_to_timestamp(utc)
                if t < latest_time:
                    tools.filesys.touch(file_path + '/compress')
                    self.log.write('Hour ' + date + 'T' + hour + ' scheduled for compression.')


        # Write the concluding statistics to the log file
        self.log_and_output('Processed ' + str(self.n_total) + ' files.')
        self.log_and_output(str(self.n_corrupt) + ' corrupt files.')
        self.log_and_output(str(self.n_copied) + ' copied files.')
        self.log_and_output(str(self.n_skipped) + ' duplicate files.')



