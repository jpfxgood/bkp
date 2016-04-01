# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
""" module to implement high level functions for file systems, s3, or ssh """
import s3_mod
import ssh_mod
import file_mod
import bkp_conf
import time
import re

def fs_utime( remote_path, times, get_config=bkp_conf.get_config ):
    """ use the appropriate function to set the access and modified times on a file """
    if remote_path.startswith("ssh://"):
        return ssh_mod.ssh_utime( remote_path, times, get_config )
    elif remote_path.startswith("s3://"):
        raise Exception("Not supported for s3 yet!")
    elif remote_path.startswith("file://"):
        return file_mod.file_utime( remote_path, times )
    else:
        raise Exception("fs_get: Unknown remote file system",remote_path)


def fs_get( remote_path, local_path, get_config=bkp_conf.get_config ):
    """ use the appropriate function to copy a file from the remote_path to the local_path """
    if remote_path.startswith("ssh://"):
        return ssh_mod.ssh_get( remote_path, local_path, get_config )
    elif remote_path.startswith("s3://"):
        return s3_mod.s3_get( remote_path, local_path )
    elif remote_path.startswith("file://"):
        return file_mod.file_get( remote_path, local_path )
    else:
        raise Exception("fs_get: Unknown remote file system",remote_path)

def fs_put( local_path, remote_path, get_config=bkp_conf.get_config, verbose = False  ):
    """ use the appropriate function to copy a file from the local_path to the remote_path """
    if remote_path.startswith("ssh://"):
        return ssh_mod.ssh_put( local_path, remote_path, get_config, verbose)
    elif remote_path.startswith("s3://"):
        return s3_mod.s3_put( local_path, remote_path)
    elif remote_path.startswith("file://"):
        return file_mod.file_put( local_path, remote_path )
    else:
        raise Exception("fs_put: Unknown remote file system",remote_path)
    

def fs_ls( remote_path, recurse=False, get_config = bkp_conf.get_config ):
    """ use the appropriate function to get a file listing of the path """
    if remote_path.startswith("ssh://"):
        return ssh_mod.ssh_ls( remote_path, recurse, get_config)
    elif remote_path.startswith("s3://"):
        return s3_mod.s3_ls( remote_path, recurse)
    elif remote_path.startswith("file://"):
        return file_mod.file_ls( remote_path, recurse )
    else:
        raise Exception("fs_ls: Unknown remote file system",remote_path)
    

def fs_del( remote_path, recurse=False, get_config=bkp_conf.get_config ):
    """ use the appropriate function to delete a file or directory at the path """
    if remote_path.startswith("ssh://"):
        return ssh_mod.ssh_del( remote_path, recurse, get_config)
    elif remote_path.startswith("s3://"):
        return s3_mod.s3_del( remote_path, recurse)
    elif remote_path.startswith("file://"):
        return file_mod.file_del( remote_path, recurse )
    else:
        raise Exception("fs_del: Unknown remote file system",remote_path)
        

def fs_stat( remote_path, get_config = bkp_conf.get_config ):
    """ return tuple ( mtime, size ) for a path to a file, returns (-1,-1) if doesn't exist """
    if remote_path.startswith("ssh://"):
        return ssh_mod.ssh_stat( remote_path, get_config )
    elif remote_path.startswith("file://"):
        return file_mod.file_stat( remote_path )
    elif remote_path.startswith("s3://"):
        ls_out = fs_ls( remote_path, False, get_config )
        if not ls_out:
            return (-1,-1)
    
        mtime = time.mktime(time.strptime(ls_out[:16],"%Y-%m-%d %H:%M"))
        parts = re.split("\s+",ls_out,3)
        size = int(parts[2])
    
        return (mtime, size)              
    else:
        raise Exception("fs_stat: Unknown remote file system", remote_path )
    
def fs_test( remote_path, verbose = False, get_config = bkp_conf.get_config ):
    """ use the appropriate function to test if file system is accessable  """
    if remote_path.startswith("ssh://"):
        return ssh_mod.ssh_test( remote_path, verbose, get_config)
    else:
        # TODO: add appropriate tests for s3 and local file system
        return True
