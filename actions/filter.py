import calendar, glob, os, time
from .shared_code import *
import shutil
from . import settings

class FilterAction(Action):

    def run(self):
        """Run a filter action."""
        self.output('Running filter action.')
        self.log.write('Running filter action.')
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
        for subdir, dirs, files in os.walk(self.root_dir + settings.downloaded_dir):
            for file_name in files:
                file_path = os.path.join(subdir, file_name)
                # Attempt to interpret the file as a feed download
                # First, extract the UID from the file name and see if such a UID exists
                # Double check by comparing the file extension
                # If it's not a valid file, delete it
                uid = file_name[:file_name.find('-')]
                ext = file_name[file_name.rfind('.')+1:]    
                if uid not in feeds_by_uid or ext != feeds_by_uid[uid][2]:
                    os.remove(file_path)
                    self.log.write('Deleted zombie file ' + file_path)
                    continue

                # Check the last modification time; if it was within file_access_lag seconds ignore this file to avoid file I/O clash with a download action
                if(time.time()-os.path.getmtime(file_path) < self.file_access_lag):
                    continue
                self.n_total += 1

                # Try to use the user-defined function to determine the feed's timestamp
                # If there is an exception raised, or if the timestamp is negative, mark the file as corrupt 
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

                # Calculate the target directory and target filename within the filtered store
                t = timestamp_to_data_list(timestamp)
                target_dir = self.root_dir + settings.filtered_dir + t[0] + '-' + t[1] + '-' + t[2] + '/' + t[3] + '/' + uid + '/'
                target_file_name = uid + '-' + timestamp_to_utc_8601(timestamp) + '.' + ext
                ensure_dir(target_dir)
                if timestamp > last_download_time:
                    last_download_time = timestamp

                
                # If the target file does not exist, copy it
                # Otherwise this is a duplicate file and can be skipped
                if not os.path.isfile(target_dir + target_file_name):
                    self.log.write('Copying: ' + file_path)
                    self.log.write('    to ' + target_dir + target_file_name)
                    shutil.copyfile(file_path, target_dir + target_file_name)
                    self.n_copied += 1
                else:
                    self.log.write('Skipping (duplicate):' + file_path)
                    self.n_skipped += 1

                # Remove the original file
                os.remove(file_path)

                # Check if the limit has been reached
                if self.limit >= 0 and self.n_total >= self.limit:
                    limit_reached = True
                    self.log.write('Copy threshold reached; closing.')
                    self.output('Copy threshold reached; closing.')
                    self.output('Run again to filter more files.')
                    break

            # We're in a double for loop here, so need to break out of the second one
            if self.limit >= 0 and self.n_total >= self.limit:
                break

        # Delete empty subdirectories in the downloaded store
        total = remove_empty_directories(self.root_dir + settings.downloaded_dir)
        self.log.write('Removed ' + str(total) + ' empty directories in the downloaded store.')

        if last_download_time >= 0:
            if limit_reached == True:
                self.record_downloaded_timestamp(last_download_time)
            else:
                self.schedule_compressions(last_download_time)


        # Write the concluding statistics to the log file
        self.log.write('Processed ' + str(self.n_total) + ' files')
        self.log.write(str(self.n_corrupt) + ' corrupt files')
        self.log.write(str(self.n_copied) + ' copied files')
        self.log.write(str(self.n_skipped) + ' skipped files (duplicates of copied files)')
        self.output('Processed ' + str(self.n_total) + ' files')
        self.output(str(self.n_corrupt) + ' corrupt files')
        self.output(str(self.n_copied) + ' copied files')
        self.output(str(self.n_skipped) + ' skipped files (duplicates of copied files)')



    def latest_timestamp_on_file(self):
        latest = -1
        for timestamp in os.listdir(self.root_dir + settings.filtered_dir + 'times/'):
            if int(timestamp)>latest:
                latest = int(timestamp)
        return latest


    def record_downloaded_timestamp(self, timestamp):
        print('recording')
        existing = self.latest_timestamp_on_file()
        if timestamp > existing:
            try:
                open(self.root_dir + settings.filtered_dir + 'times/' + str(timestamp), 'x')
            except FileExistsError:
                pass
            for old_timestamp in os.listdir(self.root_dir + settings.filtered_dir + 'times/'):
                if int(old_timestamp)<timestamp:
                    os.remove(self.root_dir + settings.filtered_dir + 'times/' + old_timestamp)
        pass

    def schedule_compressions(self, timestamp):
        existing = self.latest_timestamp_on_file()
        timestamp = max(timestamp, existing)
        print('Timestamp')
        print(timestamp)
        # Iterate over all the filtered directories
        # If the corresponding hour is before the timestamp, then schedule for compression
        files = glob.glob(self.root_dir + settings.filtered_dir + '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/[0-9][0-9]')    
        for file_path in files:
            hour = file_path[-2:]
            date = file_path[-13:-3]
            utc = date + 'T' + hour + '5959Z'
            t = utc_8601_to_timestamp(utc)
            if t < timestamp:
                # Schedule for compression
                try:
                    open(file_path + '/compress', 'x')
                except FileExistsError:
                    pass

        for old_timestamp in os.listdir(self.root_dir + settings.filtered_dir + 'times/'):
            os.remove(self.root_dir + settings.filtered_dir + 'times/' + old_timestamp)
        pass
