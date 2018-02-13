"""Provides operations on the local file system, such as
creating and extracting tar files, and creating directories."""

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
    """Ensure that the directory given by path exists.

    If the directory exists, do nothing, otherwise create it.
    """

    d = os.path.dirname(path)
    if not os.path.exists(d):
        os.makedirs(d)


def directory_to_tar_file(directory, tar_file, overwrite=False):
    """Compress the contents of the given directory into a tar file.

    The contents of the directory and the directory itself will be deleted
    after compression. Unless overwrite is True, a FileExistsError exception
    will be thrown if tar_file already exists.
    """

    # The exception throwing is determined simply by the file opening mode.
    if overwrite is True:
        s = 'w'
    else:
        s = 'x'
    tar_handle = tarfile.open(tar_file, s+':bz2')
    tar_handle.add(directory, arcname='')
    tar_handle.close()
    shutil.rmtree(directory)


def tar_file_to_directory(tar_file, directory):
    """Extract the given tar archive into a directory.

    The tar file will be deleted after unpacking.
    """

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


def prune_directory_tree(dir_path, delete_self=False):
    """Remove all directories that do not contain files in their directory trees.

    If delete_self is True, also delete dir_path if the tree contains no files.
    """

    # The pruning occurs by a depth first search, implemented
    # through recursive calls to this function.

    # The variable contains_files will tell whether the current directory
    # contains any files or directories with files in their trees.
    contains_files = False
    # total counts the number of directory deletions.
    total = 0
    # Iterate through each child of the current node.
    for entry in os.listdir(dir_path):
        path = dir_path + entry
        # If the child is a directory, traverse down it recursively.
        # The delete_self flag is true, meaning the child itself will be
        # deleted if there are no files in its directory tree.
        if os.path.isdir(path):
            total += prune_directory_tree(path + '/', True)
            if os.path.isdir(path + '/'):
                contains_files = True
        # If this child is not a directory, the node will not be deleted.
        else:
            contains_files = True
    # Based on the result, delete the present node or not.
    if contains_files is False and delete_self is True:
        os.rmdir(dir_path)
        total += 1
    return total
