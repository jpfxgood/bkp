# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
""" module to implement shared functions for the rstr tool """
import sys
import os
import tempfile
import re
import traceback
import queue
import threading
import platform
import time
import subprocess
import datetime
import urllib.request, urllib.parse, urllib.error
import io
from bkp_core import bkp_mod
from bkp_core import bkp_conf
from bkp_core.util import get_contents
from bkp_core.fs_mod import fs_get,fs_put,fs_ls
from bkp_core.logger import start_logger, stop_logger, wait_for_logger, log

restore_worker_thread_pool = []
restore_work_queue = queue.Queue()
restore_workers_stop = False


def perform_restore():
    """ worker thread body that pulls restore actions off the queue and performs them """
    while not restore_workers_stop:
        try:
            params = restore_work_queue.get(True,1)
            try:
                if not bkp_mod.dryrun:
                    fs_get( params.remote_path, params.local_path )
                    os.utime( params.local_path, (params.time, params.time))
                log( "Restored %s to %s"%(params.remote_path,params.local_path))
                restore_work_queue.task_done()
            except:
                tb = traceback.format_exc()
                log( tb )
                restore_work_queue.task_done()
        except queue.Empty:
            continue

def start_restore_workers():
    """ start the right number of restore worker threads to perform the restoring """
    global restore_worker_thread_pool
    num_threads = int(bkp_conf.get_config()["threads"])

    while num_threads:
        t = threading.Thread(target=perform_restore)
        t.start()
        restore_worker_thread_pool.append(t)
        num_threads = num_threads - 1

def stop_restore_workers():
    """ set a flag that causes all of the restore workers to exit """
    global restore_workers_stop
    restore_workers_stop = True

def wait_for_restore_workers():
    """ wait until the restore queue clears """
    global restore_worker_thread_pool
    global restore_worker_stop
    global restore_work_queue

    if not restore_work_queue.empty():
        restore_work_queue.join()
    stop_restore_workers()
    for r in restore_worker_thread_pool:
        if r.is_alive():
            r.join()
    restore_worker_thread_pool = []
    restore_worker_stop = False
    restore_work_queue = queue.Queue()





class Restore:
    """ class to represent parameters about a restore candidate """

    def __init__(self, r_path, l_path, t_path, time ):
        """ constructor takes remote path, orignal local path, target local path, and the for the file as a float """
        self.remote_path = r_path
        self.original_path = l_path
        self.local_path = t_path
        self.time = time


def restore( machine=platform.node(), restore_path = "", exclude_pats = [], asof = "", restore_pats = [] ):
    """ main restore driver, will loop over all backups for this server and restore all files to the restore path that match the restore_pats and are not excluded by the exlcude patterns up to the asof date """
    try:
        # start the logger
        start_logger()

        # expand user path references in restore_path
        restore_path = os.path.expanduser(restore_path)

        # if asof is not specified then restore as of now
        if not asof:
            end_time_t = time.localtime(time.time())
            asof = "%04d.%02d.%02d.%02d.%02d.%02d"%(end_time_t.tm_year, end_time_t.tm_mon, end_time_t.tm_mday, end_time_t.tm_hour, end_time_t.tm_min, end_time_t.tm_sec)

        # get asof as a time value
        asof_time = bkp_mod.timestamp2time( asof )

        # the backups for a given machine will be in s3://bucket/bkp/machine_name
        machine_path = bkp_conf.get_config()["bucket"]+"/bkp/"+machine

        try:
            # get backup paths and timestamps returns Backup objects with  (time, timestamp, path)
            backups = bkp_mod.get_backups( machine_path )

            # loop over the backups, process the log files and collect the correct versions of matching files
            restore_map = {}
            for bk in backups:
                if bkp_mod.verbose:
                    log("Examining backup: %s"%(bk.path))

                # if the backup is after the asof date then skip it
                if bk.time > asof_time:
                    if bkp_mod.verbose:
                        log("Skipping because it is newer than asof backup: %s"%(bk.path))
                    continue

                # fetch the contents of the backup log
                contents = get_contents(machine_path,bk.timestamp+"/bkp/bkp."+bk.timestamp+".log",bkp_mod.verbose)

                # collect the newest version less than the asof time and apply all the filters
                # if there's a backup log then we do this the easy way
                if contents:
                    if bkp_mod.verbose:
                        log("Found log file and processing it")

                    past_config = False
                    for l in io.StringIO(contents):
                        if not past_config:
                            if l.startswith("end_config"):
                                past_config = True
                        else:
                            local_path,remote_path,status,msg = l.split(";",3)
                            if status == "error":
                                if bkp_mod.verbose:
                                    log("Skipping because of error: %s"%(local_path))
                                continue
                            if local_path in restore_map and restore_map[local_path].time > bk.time:
                                if bkp_mod.verbose:
                                    log("Skipping because we already have a newer one: %s"%(local_path))
                                continue
                            exclude = False
                            for ex in exclude_pats:
                                if re.match(ex,local_path):
                                    exclude = True
                                    break
                            if exclude:
                                if bkp_mod.verbose:
                                    log("Skipping because of exclude %s %s"%(ex,local_path))
                                continue
                            restore = False
                            for rs in restore_pats:
                                if re.match(rs,local_path):
                                    restore = True
                                    break
                            if not restore:
                                if bkp_mod.verbose:
                                    log("Skipping because not included: %s"%(local_path))
                                continue
                            if bkp_mod.verbose:
                                log("Including: %s"%(local_path))

                            restore_map[local_path] = Restore(remote_path,local_path,os.path.join(restore_path,local_path[1:]),bk.time)
                else:
                    if bkp_mod.verbose:
                        log("No log file doing a recursive ls of %s"%bk.path)
                    # ok this is a screwed up one that doesn't have a log so recurse using ls and build the list off of that
                    for l in io.StringIO(fs_ls(bk.path,True)):
                        prefix,path = re.split(bk.timestamp,l)
                        path = path.strip()
                        local_path = urllib.request.url2pathname(path)
                        remote_path = bk.path + path[1:]
                        if local_path in restore_map:
                            if bkp_mod.verbose:
                                log( "Found in map: %s"%(local_path))
                            if restore_map[local_path].time > bk.time:
                                if bkp_mod.verbose:
                                    log("Skipping because we already have a newer one %s"%(local_path))
                                continue
                        exclude = False
                        for ex in exclude_pats:
                            if re.match(ex,local_path):
                                exclude = True
                                break
                        if exclude:
                            if bkp_mod.verbose:
                                log("Skipping because of exclude %s %s"%(ex,local_path))
                            continue
                        restore = False
                        for rs in restore_pats:
                            if re.match(rs,local_path):
                                restore = True
                                break
                        if not restore:
                            if bkp_mod.verbose:
                                log("Skipping because not included %s"%(local_path))
                            continue
                        if bkp_mod.verbose:
                            log("Including: %s"%(local_path))
                        restore_map[local_path] = Restore(remote_path,local_path,os.path.join(restore_path,local_path[1:]),bk.time)
        except:
            log("Exception while processing: "+traceback.format_exc())

        # if we have things to restore then go for it
        if restore_map:
            # start up the restore workers
            start_restore_workers()

            # enqueue all of the restore tasks
            for rest in restore_map.values():
                restore_work_queue.put(rest)

            # wait for the restore workers
            wait_for_restore_workers()

        # wait for logging to complete
        wait_for_logger()
    except:
        # stop the restore workers
        stop_restore_workers()

        # stop the restore logger
        stop_logger()
        raise

