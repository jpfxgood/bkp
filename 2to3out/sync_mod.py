# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
""" module to implement shared functions for the sync tool """
import sys
import os
import re
import traceback
import queue
import threading
import platform
import time
import subprocess
import smtplib
import datetime
import io
import urllib.request, urllib.parse, urllib.error
from fs_mod import fs_get,fs_put,fs_ls,fs_del,fs_stat,fs_test,fs_utime
from sync_conf import save_config,config,get_config
from util import get_contents, put_contents
from logger import start_logger, stop_logger, wait_for_logger, log

dryrun = False
verbose = False
work_queue = queue.Queue()
machine_path = ""
errors_count = 0
worker_thread_pool = [] 
processed_files = {}
processed_dirs = {}
pending_markers = []
worker_stop = False
remote_processed_files = {}
remote_processed_files_name = os.path.expanduser("~/.sync/.sync.processed")

def set_dryrun( dr ):
    """ set the dryrun flag to true to prevent real actions in s3 """
    global dryrun
    dryrun = dr
    
def set_verbose( vb ):
    """ set the verbose flag to true to enable extended output """
    global verbose
    verbose = vb

class WorkerParams:
    """ worker params """
    def __init__(self, method, from_path, to_path, mtime = 0.0):
        """ set up the copy from and to paths for the worker """
        self.from_path = from_path
        self.to_path = to_path
        self.method = method
        self.mtime = mtime
        
def process_sync():
    """ thread body for worker thread, loop processing the queue until killed """
    global errors_count
    start_time = time.time()
    while not worker_stop:
        try:
            # every 5 minutes dump a stack trace if verbose
            if time.time() - start_time > 300:
                start_time = time.time()
            params = work_queue.get(True,1)
            try:      
                if not dryrun:
                    if verbose:
                        log( "Starting transfer: %s to %s"%(params.from_path, params.to_path) )
                    if params.method == fs_put:
                        params.method( params.from_path, params.to_path, get_config, verbose)
                        fs_utime( params.to_path, (params.mtime, params.mtime), get_config)
                    else:
                        params.method( params.from_path, params.to_path, get_config )
                        os.utime( params.to_path, (params.mtime, params.mtime))
                if verbose:
                    log( "Transferred: %s to %s"%(params.from_path, params.to_path) )
                work_queue.task_done()
            except:
                tb = traceback.format_exc()
                log( "Failed Transfer: %s to %s error %s"%(params.from_path, params.to_path, tb) )
                errors_count += 1
                work_queue.task_done()
        except queue.Empty:
            continue
        except:
            work_queue.task_done()
            log(traceback.format_exc())
            continue
            
def start_workers():
    """ start the workers in the pool """
    global worker_thread_pool
    num_threads = int(get_config()["threads"])
    
    while num_threads:
        t = threading.Thread(target=process_sync)
        t.start()
        worker_thread_pool.append(t)
        num_threads = num_threads - 1
    
def stop_workers():
    """ stop the workers """
    global worker_stop
    worker_stop = True
    
def wait_for_workers():
    """ wait for the worker queue to be empty """
    if not work_queue.empty():
        work_queue.join()
    stop_workers()
    for t in worker_thread_pool:
        if t.is_alive():
            t.join()
    
def sync_directory( path ):
    """ enqueue the files to be synced for a given directory path, apply filters on datetime, pattern, non-hidden files only, recurse visible subdirs """
    
    # save off remote directory recursive listing
    remote_files = fs_ls(machine_path+path,True, get_config)
    
  
    
    for (dirpath, dirnames, filenames) in os.walk(path):
        if verbose:
            log("Scanning dirpath= %s"%(dirpath))
        # if exclude_dirs is contained in any of the paths then return
        exclude_dir = False
        for e in get_config()["exclude_dirs"]:
            if e and re.search(e,dirpath):
                exclude_dir = True
                break
        if exclude_dir:
            if verbose:
                log("Excluding dirpath= %s because of e= %s"%(dirpath,e))
            continue
            
        # get rid of hidden directories
        while True:
            deleted = False
            didx = 0
            for d in dirnames:
                if d[0] == ".":
                    if verbose:
                        log("Deleting hidden directory = %s"%(d))
                    del dirnames[didx]
                    deleted = True
                    break
                didx = didx + 1
            if not deleted:
                break                                                
                
        # stat the sentinel file .sync to avoid sloshing files around
        sync_marker_path = machine_path + os.path.abspath(dirpath)
        sync_marker_node = ".sync."+ platform.node()
        sync_marker = os.path.join( sync_marker_path, sync_marker_node )
        sync_mtime,sync_size = fs_stat(sync_marker,get_config)
        
        # process files in the directory enqueueing included files for sync
        for f in filenames:
            # if it is a hidden file skip it
            if f[0] == ".":
                if verbose:
                    log("Skipping hidden file = %s"%(f))
                continue                   
                
            # if it is excluded file skip it
            if get_config()["exclude_files"] and re.match(get_config()["exclude_files"],f):
                if verbose:
                    log("Excluding file = %s Because of pattern= %s"%(f,get_config()["exclude_files"]))
                continue
                                            
            # build the absolute path for the file and it's sync path
            local_path = os.path.join(os.path.abspath(dirpath),f)
            remote_path = machine_path + local_path
            
            # if the file is in the time range for this sync then queue it for sync
            s = os.lstat(local_path)
            mtime, size = fs_stat(remote_path,get_config)
            processed_files[remote_path] = True
            
            if s.st_mtime < mtime and (mtime - s.st_mtime) >= 1.0:
                if verbose:
                    log("Enqueuing get for %s,%s timediff %f"%(remote_path,local_path, mtime - s.st_mtime))
                work_queue.put(WorkerParams( fs_get, remote_path, local_path, mtime ))
            elif s.st_mtime > mtime and (s.st_mtime - mtime) >= 1.0:
                if verbose:
                    log("Enqueuing put for %s,%s timediff %f"%(local_path,remote_path,s.st_mtime - mtime))
                work_queue.put(WorkerParams( fs_put, local_path, remote_path, s.st_mtime ))
            else:
                if verbose:
                    log("Not Enqueuing copy work for %s because time is the same or not greater than last sync"%(local_path))
        # drop a marker file on the remote host
        pending_markers.append((sync_marker_path,sync_marker_node))
        processed_dirs[sync_marker_path] = True

    if verbose:
        log("Checking for files only present on the server")
        log(remote_files)
        
    #loop over remote files and handle any that haven't already been synced
    for line in io.StringIO(remote_files):
        fdate,ftime,size,fpath = re.split("\s+",line,3)
        fpath = fpath.strip()
        if not fpath in processed_files:
            lpath = fpath[len(machine_path):]
            ldir,lnode = os.path.split(lpath)
            fdir,fnode = os.path.split(fpath)
            # if exclude_dirs is contained in any of the paths then return
            exclude_dir = False
            for e in get_config()["exclude_dirs"]:
                if (e and re.search(e,ldir)) or re.match(".*/\..*",ldir):
                    exclude_dir = True
                    break
            if exclude_dir:
                if verbose:
                    log("Excluding dirpath= %s because of e= %s"%(ldir,e))
                continue                                           
            # if it is a hidden file skip it
            if lnode[0] == ".":
                if verbose:
                    log("Skipping hidden file = %s"%(lnode))
                continue                   
                
            # if it is excluded file skip it
            if get_config()["exclude_files"] and re.match(get_config()["exclude_files"],lnode):
                if verbose:
                    log("Excluding file = %s Because of pattern= %s"%(lnode,get_config()["exclude_files"]))
                continue     
                
            # if it was processed in the past don't fetch it just mark it as processed
            # it was deleted on the client otherwise enqueue a get
            if not fpath in remote_processed_files:
                if verbose:
                    log("Enqueuing get for %s,%s"%(fpath,lpath))
                mtime, size = fs_stat(fpath,get_config)
                work_queue.put(WorkerParams( fs_get, fpath, lpath, mtime))
            else:
                if verbose:
                    log("Not enqueuing get for %s becase it was deleted on client"%(fpath))
            processed_files[fpath] = True
            if not fdir in processed_dirs:
                processed_dirs[fdir] = True
                pending_markers.append((fdir,".sync."+platform.node()))
            
    return

def synchronize():
    """ driver to perform syncrhonize """
    global machine_path, errors_count
                                                 
    try:  
        # the sync target a given machine will be target
        machine_path = get_config()["target"]
        
        # if there is no connection to the target then exit
        if not fs_test( machine_path, verbose, get_config ):
            return 0
    
        # get the remote processed files so we can check for deletes
        if os.path.exists(remote_processed_files_name):
            for line in open(remote_processed_files_name):
                remote_processed_files[line.strip()] = True
            
        # start the logger thread
        start_logger()
    
        # fire up the worker threads
        start_workers()
    
        # loop over the paths provided and add them to the work queue
        for d in get_config()["dirs"]:
            sync_directory( d )
    
        # wait for queue to empty
        wait_for_workers()
        
        # drop all our sync markers after any copies complete
        for sync_marker_path,sync_marker_node in pending_markers:
            put_contents(sync_marker_path,sync_marker_node, "syncrhonized %s"%time.ctime(),dryrun,get_config,verbose)
            
        # write out the processed files
        if not dryrun:
            processed_out = open(remote_processed_files_name,"w")
            for fpath in processed_files.keys():
                print(fpath, file=processed_out)
            processed_out.close()

        # wait for the logger to finish
        wait_for_logger()
        
    finally:
        stop_workers()
        stop_logger()
    
    if errors_count:
        return 1
    else:
        return 0
