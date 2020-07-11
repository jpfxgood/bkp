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
from bkp_core.logger import Logger

class Restore:
    """ class to represent parameters about a restore candidate """

    def __init__(self, r_path, l_path, t_path, time ):
        """ constructor takes remote path, orignal local path, target local path, and the for the file as a float """
        self.remote_path = r_path
        self.original_path = l_path
        self.local_path = t_path
        self.time = time

class RestoreJob:
    def __init__( self, config):
        self.dryrun = False
        self.verbose = False
        self.init( config )

    def init( self, config ):
        """ initialize the internal state for another run """
        self.config = config
        self.restore_worker_thread_pool = []
        self.restore_work_queue = queue.Queue()
        self.restore_workers_stop = False
        self.logger = Logger()

    def set_dryrun( self, dr ):
        """ set the dryrun flag to true to have it just log all the actions but not perform them """
        self.dryrun = dr

    def set_verbose( self, vb ):
        """ set the verbose flag to log a lot more detail about the process """
        self.verbose = vb

    def perform_restore( self ):
        """ worker thread body that pulls restore actions off the queue and performs them """
        while not self.restore_workers_stop:
            try:
                params = self.restore_work_queue.get(True,1)
                try:
                    if not self.dryrun:
                        fs_get( params.remote_path, params.local_path, lambda: self.config )
                        os.utime( params.local_path, (params.time, params.time))
                    self.logger.log( "Restored %s to %s"%(params.remote_path,params.local_path))
                    self.restore_work_queue.task_done()
                except:
                    tb = traceback.format_exc()
                    self.logger.log( tb )
                    self.restore_work_queue.task_done()
            except queue.Empty:
                continue

    def start_restore_workers(self):
        """ start the right number of restore worker threads to perform the restoring """
        num_threads = int(self.config["threads"])

        while num_threads:
            t = threading.Thread(target=self.perform_restore)
            t.start()
            self.restore_worker_thread_pool.append(t)
            num_threads = num_threads - 1

    def stop_restore_workers(self):
        """ set a flag that causes all of the restore workers to exit """
        self.restore_workers_stop = True

    def wait_for_restore_workers(self):
        """ wait until the restore queue clears """

        if not self.restore_work_queue.empty():
            self.restore_work_queue.join()
        self.stop_restore_workers()
        for r in self.restore_worker_thread_pool:
            if r.is_alive():
                r.join()
        self.restore_worker_thread_pool = []
        self.restore_worker_stop = False
        self.restore_work_queue = queue.Queue()

    def restore( self, machine=platform.node(), restore_path = "", exclude_pats = [], asof = "", restore_pats = [] ):
        """ main restore driver, will loop over all backups for this server and restore all files to the restore path that match the restore_pats and are not excluded by the exlcude patterns up to the asof date """
        try:
            # initialize our internal state to begin a new run
            self.init(self.config)

            # start the logger
            self.logger.start_logger()

            # expand user path references in restore_path
            restore_path = os.path.expanduser(restore_path)

            # if asof is not specified then restore as of now
            if not asof:
                end_time_t = time.localtime(time.time())
                asof = "%04d.%02d.%02d.%02d.%02d.%02d"%(end_time_t.tm_year, end_time_t.tm_mon, end_time_t.tm_mday, end_time_t.tm_hour, end_time_t.tm_min, end_time_t.tm_sec)

            # get asof as a time value
            asof_time = bkp_mod.timestamp2time( asof )

            # the backups for a given machine will be in s3://bucket/bkp/machine_name
            machine_path = self.config["bucket"]+"/bkp/"+machine

            try:
                # get backup paths and timestamps returns Backup objects with  (time, timestamp, path)
                backups = bkp_mod.get_backups( machine_path, self.config, self.verbose )

                # loop over the backups, process the log files and collect the correct versions of matching files
                restore_map = {}
                for bk in backups:
                    if self.verbose:
                        self.logger.log("Examining backup: %s"%(bk.path))

                    # if the backup is after the asof date then skip it
                    if bk.time > asof_time:
                        if self.verbose:
                            self.logger.log("Skipping because it is newer than asof backup: %s"%(bk.path))
                        continue

                    # fetch the contents of the backup log
                    contents = get_contents(machine_path,bk.timestamp+"/bkp/bkp."+bk.timestamp+".log",self.verbose, lambda: self.config)

                    # collect the newest version less than the asof time and apply all the filters
                    # if there's a backup log then we do this the easy way
                    if contents:
                        if self.verbose:
                            self.logger.log("Found log file and processing it")

                        past_config = False
                        for l in io.StringIO(contents):
                            if not past_config:
                                if l.startswith("end_config"):
                                    past_config = True
                            else:
                                local_path,remote_path,status,msg = l.split(";",3)
                                if status == "error":
                                    if self.verbose:
                                        self.logger.log("Skipping because of error: %s"%(local_path))
                                    continue
                                if local_path in restore_map and restore_map[local_path].time > bk.time:
                                    if self.verbose:
                                        self.logger.log("Skipping because we already have a newer one: %s"%(local_path))
                                    continue
                                exclude = False
                                for ex in exclude_pats:
                                    if re.match(ex,local_path):
                                        exclude = True
                                        break
                                if exclude:
                                    if self.verbose:
                                        self.logger.log("Skipping because of exclude %s %s"%(ex,local_path))
                                    continue
                                restore = False
                                for rs in restore_pats:
                                    if re.match(rs,local_path):
                                        restore = True
                                        break
                                if not restore:
                                    if self.verbose:
                                        self.logger.log("Skipping because not included: %s"%(local_path))
                                    continue
                                if self.verbose:
                                    self.logger.log("Including: %s"%(local_path))

                                restore_map[local_path] = Restore(remote_path,local_path,os.path.join(restore_path,local_path[1:]),bk.time)
                    else:
                        if self.verbose:
                            self.logger.log("No log file doing a recursive ls of %s"%bk.path)
                        # ok this is a screwed up one that doesn't have a log so recurse using ls and build the list off of that
                        for l in io.StringIO(fs_ls(bk.path,True, lambda: self.config)):
                            prefix,path = re.split(bk.timestamp,l)
                            path = path.strip()
                            local_path = urllib.request.url2pathname(path)
                            remote_path = bk.path + path[1:]
                            if local_path in restore_map:
                                if self.verbose:
                                    self.logger.log( "Found in map: %s"%(local_path))
                                if restore_map[local_path].time > bk.time:
                                    if self.verbose:
                                        self.logger.log("Skipping because we already have a newer one %s"%(local_path))
                                    continue
                            exclude = False
                            for ex in exclude_pats:
                                if re.match(ex,local_path):
                                    exclude = True
                                    break
                            if exclude:
                                if self.verbose:
                                    self.logger.log("Skipping because of exclude %s %s"%(ex,local_path))
                                continue
                            restore = False
                            for rs in restore_pats:
                                if re.match(rs,local_path):
                                    restore = True
                                    break
                            if not restore:
                                if self.verbose:
                                    self.logger.log("Skipping because not included %s"%(local_path))
                                continue
                            if self.verbose:
                                self.logger.log("Including: %s"%(local_path))
                            restore_map[local_path] = Restore(remote_path,local_path,os.path.join(restore_path,local_path[1:]),bk.time)
            except:
                self.logger.log("Exception while processing: "+traceback.format_exc())

            # if we have things to restore then go for it
            if restore_map:
                # start up the restore workers
                self.start_restore_workers()

                # enqueue all of the restore tasks
                for rest in restore_map.values():
                    self.restore_work_queue.put(rest)

                # wait for the restore workers
                self.wait_for_restore_workers()

            # wait for logging to complete
            self.logger.wait_for_logger()
        except:
            # stop the restore workers
            self.stop_restore_workers()

            # stop the restore logger
            self.logger.stop_logger()
            raise
