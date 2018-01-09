"""Clear any log file that is more than 24 hours old."""

import os
import time
from tasks import settings

def clear_logs_in_tree(dir_path):
    """Clear any log file in the tree rooted in root_dir that is more than 24 hours old. Return the number of log files deleted."""

    # If the path does not exist, there is nothing to do.
    if not os.path.isdir(dir_path):
        return 0

    # Travel through the tree.
    total = 0
    for entry in os.listdir(dir_path):
        path = dir_path + entry
        # If this is a directory, look in the subtree there.
        if os.path.isdir(path):
            total += clear_logs_in_tree(path + '/')
        # If this a file, check it's a log file and that it was last modified more than 24 hours ago, and if so remove.
        else:
            if entry[entry.rfind('.'):] == '.log':
                if time.time() - os.path.getmtime(path) >= 60*60*24:
                    os.remove(path)
                    total += 1
    return total

for dir_path in settings.log_dir.values():
    print('Cleaning logs in ' + dir_path)
    total = clear_logs_in_tree(dir_path)
    print('Deleted ' + str(total) + ' logs.')

