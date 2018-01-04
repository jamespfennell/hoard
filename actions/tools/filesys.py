import os
import tarfile
import shutil
import hashlib
import functools 


def md5sum(filename):
    with open(filename, mode='rb') as f:
        d = hashlib.md5()
        for buf in iter(functools.partial(f.read, 128), b''):
            d.update(buf)
    return d.hexdigest()

def ensure_dir(path):
    """Ensure that the local directory given by path exists. If it does not exist, create it."""
    d = os.path.dirname(path)
    if not os.path.exists(d):
        os.makedirs(d)


def directory_to_tar_file(directory, tar_file, overwrite = False):
    s = 'x'
    if overwrite is True:
        s = 'w'
    tar_handle = tarfile.open(tar_file, s+':bz2')
    tar_handle.add(directory, arcname='')
    tar_handle.close()
    shutil.rmtree(directory)

def tar_file_to_directory(tar_file, directory):
    ensure_dir(directory)
    tar_handle = tarfile.open(tar_file, 'r:bz2')
    tar_handle.extractall(directory)
    tar_handle.close()
    os.remove(tar_file)

#DELETE
def silent_delete_attempt(path):
    try:
        os.rmdir(path)
    except FileNotFoundError:
        pass

def touch(file_path):
    try:
        open(file_path, 'x')
    except FileExistsError:
        pass


def remove_empty_directories(root):
    total = 0
    for subdir, dirs, files in os.walk(root, topdown = False):
        if subdir == root:
            continue
        if len(dirs) + len(files) == 0:
            os.rmdir(subdir)
            total += 1
    return total

def prune_directory_tree(dir_path, delete_self = False):
    """Delete any directories in the directory tree of dir_path that do not contain files in their subtrees.
    If delete_self is True, also delete dir_path if the tree contains no files.
    Return the total number of directories that were deleted."""

    contains_files = False
    total = 0
    for entry in os.listdir(dir_path):
        path = dir_path + entry
        if os.path.isdir(path):
            total += prune_directory_tree(path + '/', True)
            if os.path.isdir(path + '/'):
                contains_files = True
        else:
            contains_files = True
    if contains_files is False and delete_self is True:
        os.rmdir(dir_path)
        total += 1
    return total
