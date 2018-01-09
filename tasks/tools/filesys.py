"""Provides operations on the local file system, such as creating and extracting tar files, and creating directories."""

import os
import tarfile
import shutil
import hashlib
import functools 

def md5sum(filename):
    """Calculate the MD5 sum of the file located at filename."""

    with open(filename, mode='rb') as f:
        d = hashlib.md5()
        for buf in iter(functools.partial(f.read, 128), b''):
            d.update(buf)
    return d.hexdigest()

def ensure_dir(path):
    """Ensure that the local directory given by path exists by creating it if it does not exist initially."""
    d = os.path.dirname(path)
    if not os.path.exists(d):
        os.makedirs(d)

def directory_to_tar_file(directory, tar_file, overwrite = False):
    """Place the contents of directory into a tar.bz2 archive located at tar_file, and then delete the directory and its contents.
    
    Unless overwrite is True, an exception will be thrown if tar_file already exists."""
    s = 'x'
    if overwrite is True:
        s = 'w'
    tar_handle = tarfile.open(tar_file, s+':bz2')
    tar_handle.add(directory, arcname='')
    tar_handle.close()
    shutil.rmtree(directory)

def tar_file_to_directory(tar_file, directory):
    """Extract a tar.bz2 archive located at tar_file into the directory given by directory, and delete the tar file."""
    ensure_dir(directory)
    tar_handle = tarfile.open(tar_file, 'r:bz2')
    tar_handle.extractall(directory)
    tar_handle.close()
    os.remove(tar_file)

def touch(file_path):
    """If there is not file at file_path, create an empty file there."""
    try:
        open(file_path, 'x')
    except FileExistsError:
        pass

def prune_directory_tree(dir_path, delete_self = False):
    """In the directory tree of root, remove all directories that do not contain files in their directory trees.
    
    If delete_self is True, also delete dir_path if the tree contains no files."""
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
