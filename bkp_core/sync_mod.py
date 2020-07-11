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
import io
from bkp_core.fs_mod import fs_get,fs_put,fs_ls,fs_stat,fs_test,fs_utime
from bkp_core.util import put_contents
from bkp_core.logger import Logger

class WorkerParams:
    """ worker params """
    def __init__(self, method, from_path, to_path, mtime = 0.0):
        """ set up the copy from and to paths for the worker """
        self.from_path = from_path
        self.to_path = to_path
        self.method = method
        self.mtime = mtime

class SyncJob:
    def __init__( self, config ):
        self.dryrun = False
        self.verbose = False
        self.init( config )

    def init( self, config ):
        """ initialize our internal state for a new run """
        self.config = config
        self.work_queue = queue.Queue()
        self.machine_path = ""
        self.errors_count = 0
        self.worker_thread_pool = []
        self.processed_files = {}
        self.processed_dirs = {}
        self.pending_markers = []
        self.worker_stop = False
        self.remote_processed_files = {}
        if not os.path.exists(os.path.expanduser("~/.sync")):
            os.mkdir(os.path.expanduser("~/.sync"))
        self.remote_processed_files_name = os.path.expanduser("~/.sync/.sync.processed")
        self.logger = Logger()

    def set_dryrun( self, dr ):
        """ set the dryrun flag to true to prevent real actions in s3 """
        self.dryrun = dr

    def set_verbose( self, vb ):
        """ set the verbose flag to true to enable extended output """
        self.verbose = vb

    def process_sync( self ):
        """ thread body for worker thread, loop processing the queue until killed """
        start_time = time.time()
        while not self.worker_stop:
            try:
                # every 5 minutes dump a stack trace if verbose
                if time.time() - start_time > 300:
                    start_time = time.time()
                params = self.work_queue.get(True,1)
                try:
                    if not self.dryrun:
                        if self.verbose:
                            self.logger.log( "Starting transfer: %s to %s"%(params.from_path, params.to_path) )
                        if params.method == fs_put:
                            params.method( params.from_path, params.to_path, lambda: self.config, self.verbose)
                            fs_utime( params.to_path, (params.mtime, params.mtime), lambda: self.config)
                        else:
                            params.method( params.from_path, params.to_path, lambda: self.config )
                            os.utime( params.to_path, (params.mtime, params.mtime))
                    if self.verbose:
                        self.logger.log( "Transferred: %s to %s"%(params.from_path, params.to_path) )
                    self.work_queue.task_done()
                except:
                    tb = traceback.format_exc()
                    self.logger.log( "Failed Transfer: %s to %s error %s"%(params.from_path, params.to_path, tb) )
                    self.errors_count += 1
                    self.work_queue.task_done()
            except queue.Empty:
                continue
            except:
                self.work_queue.task_done()
                self.logger.log(traceback.format_exc())
                continue

    def start_workers(self):
        """ start the workers in the pool """
        num_threads = int(self.config["threads"])

        while num_threads:
            t = threading.Thread(target=self.process_sync)
            t.start()
            self.worker_thread_pool.append(t)
            num_threads = num_threads - 1

    def stop_workers(self):
        """ stop the workers """
        self.worker_stop = True

    def wait_for_workers(self):
        """ wait for the worker queue to be empty """
        if not self.work_queue.empty():
            self.work_queue.join()
        self.stop_workers()
        for t in self.worker_thread_pool:
            if t.is_alive():
                t.join()
        self.worker_stop = False
        self.worker_thread_pool = []
        self.work_queue = queue.Queue()

    def sync_directory( self, path ):
        """ enqueue the files to be synced for a given directory path, apply filters on datetime, pattern, non-hidden files only, recurse visible subdirs """

        # save off remote directory recursive listing
        remote_files = fs_ls(self.machine_path+path,True, lambda: self.config)

        for (dirpath, dirnames, filenames) in os.walk(path):
            if self.verbose:
                self.logger.log("Scanning dirpath= %s"%(dirpath))
            # if exclude_dirs is contained in any of the paths then return
            exclude_dir = False
            for e in self.config["exclude_dirs"]:
                if e and re.search(e,dirpath):
                    exclude_dir = True
                    break
            if exclude_dir:
                if self.verbose:
                    self.logger.log("Excluding dirpath= %s because of e= %s"%(dirpath,e))
                continue

            # get rid of hidden directories
            while True:
                deleted = False
                didx = 0
                for d in dirnames:
                    if d[0] == ".":
                        if self.verbose:
                            self.logger.log("Deleting hidden directory = %s"%(d))
                        del dirnames[didx]
                        deleted = True
                        break
                    didx = didx + 1
                if not deleted:
                    break

            # stat the sentinel file .sync to avoid sloshing files around
            sync_marker_path = self.machine_path + os.path.abspath(dirpath)
            sync_marker_node = ".sync."+ platform.node()
            sync_marker = os.path.join( sync_marker_path, sync_marker_node )
            sync_mtime,sync_size = fs_stat(sync_marker,lambda: self.config)

            # process files in the directory enqueueing included files for sync
            for f in filenames:
                # if it is a hidden file skip it
                if f[0] == ".":
                    if self.verbose:
                        self.logger.log("Skipping hidden file = %s"%(f))
                    continue

                # if it is excluded file skip it
                if self.config["exclude_files"] and re.match(self.config["exclude_files"],f):
                    if self.verbose:
                        self.logger.log("Excluding file = %s Because of pattern= %s"%(f,self.config["exclude_files"]))
                    continue

                # build the absolute path for the file and it's sync path
                local_path = os.path.join(os.path.abspath(dirpath),f)
                remote_path = self.machine_path + local_path

                # if the file is in the time range for this sync then queue it for sync
                s = os.lstat(local_path)
                mtime, size = fs_stat(remote_path,lambda: self.config)
                self.processed_files[remote_path] = True

                if s.st_mtime < mtime and (mtime - s.st_mtime) >= 1.0:
                    if self.verbose:
                        self.logger.log("Enqueuing get for %s,%s timediff %f"%(remote_path,local_path, mtime - s.st_mtime))
                    self.work_queue.put(WorkerParams( fs_get, remote_path, local_path, mtime ))
                elif s.st_mtime > mtime and (s.st_mtime - mtime) >= 1.0:
                    if self.verbose:
                        self.logger.log("Enqueuing put for %s,%s timediff %f"%(local_path,remote_path,s.st_mtime - mtime))
                    self.work_queue.put(WorkerParams( fs_put, local_path, remote_path, s.st_mtime ))
                else:
                    if self.verbose:
                        self.logger.log("Not Enqueuing copy work for %s because time is the same or not greater than last sync"%(local_path))
            # drop a marker file on the remote host
            self.pending_markers.append((sync_marker_path,sync_marker_node))
            self.processed_dirs[sync_marker_path] = True

        if self.verbose:
            self.logger.log("Checking for files only present on the server")
            self.logger.log(remote_files)

        #loop over remote files and handle any that haven't already been synced
        for line in io.StringIO(remote_files):
            fdate,ftime,size,fpath = re.split("\s+",line,3)
            fpath = fpath.strip()
            if not fpath in self.processed_files:
                lpath = fpath[len(self.machine_path):]
                ldir,lnode = os.path.split(lpath)
                fdir,fnode = os.path.split(fpath)
                # if exclude_dirs is contained in any of the paths then return
                exclude_dir = False
                for e in self.config["exclude_dirs"]:
                    if (e and re.search(e,ldir)) or re.match(".*/\..*",ldir):
                        exclude_dir = True
                        break
                if exclude_dir:
                    if self.verbose:
                        self.logger.log("Excluding dirpath= %s because of e= %s"%(ldir,e))
                    continue
                # if it is a hidden file skip it
                if lnode[0] == ".":
                    if self.verbose:
                        self.logger.log("Skipping hidden file = %s"%(lnode))
                    continue

                # if it is excluded file skip it
                if self.config["exclude_files"] and re.match(self.config["exclude_files"],lnode):
                    if self.verbose:
                        self.logger.log("Excluding file = %s Because of pattern= %s"%(lnode,self.config["exclude_files"]))
                    continue

                # if it was processed in the past don't fetch it just mark it as processed
                # it was deleted on the client otherwise enqueue a get
                if not fpath in self.remote_processed_files:
                    if self.verbose:
                        self.logger.log("Enqueuing get for %s,%s"%(fpath,lpath))
                    mtime, size = fs_stat(fpath,lambda: self.config)
                    self.work_queue.put(WorkerParams( fs_get, fpath, lpath, mtime))
                else:
                    if self.verbose:
                        self.logger.log("Not enqueuing get for %s becase it was deleted on client"%(fpath))
                self.processed_files[fpath] = True
                if not fdir in self.processed_dirs:
                    self.processed_dirs[fdir] = True
                    self.pending_markers.append((fdir,".sync."+platform.node()))
        return

    def synchronize(self):
        """ driver to perform syncrhonize """

        try:
            # initialize our internal state for a new run
            self.init( self.config )

            # the sync target a given machine will be target
            self.machine_path = self.config["target"]

            # if there is no connection to the target then exit
            if not fs_test( self.machine_path, self.verbose, lambda: self.config ):
                return 0

            # get the remote processed files so we can check for deletes
            if os.path.exists(self.remote_processed_files_name):
                for line in open(self.remote_processed_files_name):
                    self.remote_processed_files[line.strip()] = True

            # start the logger thread
            self.logger.start_logger()

            # fire up the worker threads
            self.start_workers()

            # loop over the paths provided and add them to the work queue
            for d in self.config["dirs"]:
                self.sync_directory( d )

            # wait for queue to empty
            self.wait_for_workers()

            # drop all our sync markers after any copies complete
            for sync_marker_path,sync_marker_node in self.pending_markers:
                put_contents(sync_marker_path,sync_marker_node, "syncrhonized %s"%time.ctime(),self.dryrun,lambda: self.config, self.verbose)

            # write out the processed files
            if not self.dryrun:
                processed_out = open(self.remote_processed_files_name,"w")
                for fpath in self.processed_files.keys():
                    print(fpath, file=processed_out)
                processed_out.close()

            # wait for the logger to finish
            self.logger.wait_for_logger()

        except:
            self.stop_workers()
            self.logger.stop_logger()
            raise

        if self.errors_count:
            return 1
        else:
            return 0
