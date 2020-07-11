# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
""" module to implement high level functions for file systems, s3, or ssh """
from bkp_core import s3_mod
from bkp_core import ssh_mod
from bkp_core import file_mod
from bkp_core import bkp_conf
import re

def fs_utime( remote_path, times, get_config = lambda: {} ):
    """ use the appropriate function to set the access and modified times on a file """
    if remote_path.startswith("ssh://"):
        return ssh_mod.ssh_utime( remote_path, times, get_config )
    elif remote_path.startswith("s3://"):
        # Not implemented for s3, however s3 defaults to copying all
        # file attributes so we don't have to do it for our use cases
        return
    elif remote_path.startswith("file://"):
        return file_mod.file_utime( remote_path, times )
    elif re.match(r"\w*://.*",remote_path):
        raise Exception("fs_utime: Unknown remote file system",remote_path)
    else:
        return file_mod.file_utime( remote_path, times )


def fs_get( remote_path, local_path, get_config = lambda: {} ):
    """ use the appropriate function to copy a file from the remote_path to the local_path """
    if remote_path.startswith("ssh://"):
        return ssh_mod.ssh_get( remote_path, local_path, get_config )
    elif remote_path.startswith("s3://"):
        return s3_mod.s3_get( remote_path, local_path )
    elif remote_path.startswith("file://"):
        return file_mod.file_get( remote_path, local_path )
    elif re.match(r"\w*://.*",remote_path):
        raise Exception("fs_get: Unknown remote file system",remote_path)
    else:
        return file_mod.file_get( remote_path, local_path )

def fs_put( local_path, remote_path, get_config = lambda: {}, verbose = False  ):
    """ use the appropriate function to copy a file from the local_path to the remote_path """
    if remote_path.startswith("ssh://"):
        return ssh_mod.ssh_put( local_path, remote_path, get_config, verbose)
    elif remote_path.startswith("s3://"):
        return s3_mod.s3_put( local_path, remote_path)
    elif remote_path.startswith("file://"):
        return file_mod.file_put( local_path, remote_path )
    elif re.match(r"\w*://.*",remote_path):
        raise Exception("fs_put: Unknown remote file system",remote_path)
    else:
        return file_mod.file_put( local_path, remote_path )


def fs_ls( remote_path, recurse=False, get_config = lambda: {} ):
    """ use the appropriate function to get a file listing of the path """
    if remote_path.startswith("ssh://"):
        return ssh_mod.ssh_ls( remote_path, recurse, get_config)
    elif remote_path.startswith("s3://"):
        return s3_mod.s3_ls( remote_path, recurse)
    elif remote_path.startswith("file://"):
        return file_mod.file_ls( remote_path, recurse )
    elif re.match(r"\w*://.*",remote_path):
        raise Exception("fs_ls: Unknown remote file system",remote_path)
    else:
        return file_mod.file_ls( remote_path, recurse )


def fs_del( remote_path, recurse=False, get_config = lambda: {} ):
    """ use the appropriate function to delete a file or directory at the path """
    if remote_path.startswith("ssh://"):
        return ssh_mod.ssh_del( remote_path, recurse, get_config)
    elif remote_path.startswith("s3://"):
        return s3_mod.s3_del( remote_path, recurse)
    elif remote_path.startswith("file://"):
        return file_mod.file_del( remote_path, recurse )
    elif re.match(r"\w*://.*",remote_path):
        raise Exception("fs_del: Unknown remote file system",remote_path)
    else:
        return file_mod.file_del( remote_path, recurse )


def fs_stat( remote_path, get_config = lambda: {} ):
    """ return tuple ( mtime, size ) for a path to a file, returns (-1,-1) if doesn't exist resolution of mtime is seconds """
    if remote_path.startswith("ssh://"):
        return ssh_mod.ssh_stat( remote_path, get_config )
    elif remote_path.startswith("file://"):
        return file_mod.file_stat( remote_path )
    elif remote_path.startswith("s3://"):
        return s3_mod.s3_stat( remote_path )
    elif re.match(r"\w*://.*",remote_path):
        raise Exception("fs_stat: Unknown remote file system", remote_path )
    else:
        return file_mod.file_stat( remote_path )

def fs_test( remote_path, verbose = False, get_config = lambda: {} ):
    """ use the appropriate function to test if file system is accessable, does NOT mean the path exists just that a host is listening  """
    if remote_path.startswith("ssh://"):
        return ssh_mod.ssh_test( remote_path, verbose, get_config)
    elif remote_path.startswith("s3://"):
        return s3_mod.s3_test( remote_path, verbose )
    else:
        # We assume the filesystem is always available
        return True
