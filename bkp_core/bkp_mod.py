# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
""" module to implement shared functions for the bkp tool """
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
from bkp_core import fs_mod
from bkp_core import bkp_conf
from bkp_core.util import get_contents, put_contents, mail_error, mail_log
from bkp_core.logger import Logger

class WorkerParams:
    """ worker params """
    def __init__(self, from_path, to_path):
        """ set up the copy from and to paths for the worker """
        self.from_path = from_path
        self.to_path = to_path


def get_machines(base_path, config):
    """ get all the machines that there are backups for """
    machines = []
    ls_output = io.StringIO(fs_mod.fs_ls(base_path, False, lambda: config ))
    for l in ls_output:
        m = re.match("(\s*DIR\s*)(\S*)$",l)
        if m:
            machines.append(os.path.split(os.path.split(m.group(2))[0])[1])
    return machines

class Backup:
    """ class to represent a backup that is available contains, path, timestamp, time for a backup """
    def __init__(self, p, ts, t ):
        """ constructor takes path, timestamp string, and time in seconds as float """
        self.path = p
        self.timestamp = ts
        self.time = t

def timestamp2time( timestamp ):
    """ convert a timestamp in yyyy.mm.dd.hh.mm.ss format to seconds for comparisons """
    return time.mktime(time.strptime(timestamp,"%Y.%m.%d.%H.%M.%S"))

def get_backups( machine_path, config, verbose = False ):
    """ get a list of all of the backups for this machine, returns list of Backup classes """
    # make sure the path ends in a / so we get the contents and not the directory itself
    if machine_path[-1] != '/':
        machine_path = machine_path + '/'

    # loop over the ls output and extract the paths and then parse out and evaluate the timestamps
    backups = []
    try:
        ls_output = io.StringIO(fs_mod.fs_ls(machine_path,False,lambda: config))
    except:
        if verbose:
            print("Error in get_backups probably no backups ", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
        ls_output = io.StringIO()

    for l in ls_output:
        m = re.match("(\s*DIR\s*)(\S*)$",l)
        if m:
            path = m.group(2)
            timestamp = os.path.split(os.path.split(m.group(2))[0])[1]
            backups.append(Backup(path,timestamp,timestamp2time(timestamp)))

    return backups

def get_backedup_files( machine_path, config, verbose = False ):
    """ return a dict with all of the files we've backed up for this machine """
    backedup = {}
    backups = get_backups( machine_path, config, verbose )
    for bk in backups:
        # fetch the contents of the backup log
        contents = get_contents(machine_path,bk.timestamp+"/bkp/bkp."+bk.timestamp+".log",verbose, lambda: config)

        # collect the newest version
        if contents:
            if verbose:
                print("Found log file and processing it", file=sys.stderr)

            past_config = False
            for l in io.StringIO(contents):
                if not past_config:
                    if l.startswith("end_config"):
                        past_config = True
                elif l.strip():
                    local_path,remote_path,status,msg = l.strip().split(";",3)
                    if local_path in backedup:
                        backedup[local_path].append( bk.time )
                    else:
                        backedup[local_path] = [bk.time]
        else:
            # ok this is a screwed up one that doesn't have a log so recurse using ls and build the list off of that
            for l in io.StringIO(fs_mod.fs_ls(bk.path,True,lambda: config)):
                prefix,path = re.split(bk.timestamp,l)
                path = path.strip()
                local_path = urllib.request.url2pathname(path)
                if local_path in backedup:
                    backedup[local_path].append(bk.time)
                else:
                    backedup[local_path] = [bk.time]

    return backedup

def list( config, verbose = False ):
    """ generate a listing of all of the files backed up for this machine with the dates available """
    # the backups for a given machine will be in s3://bucket/bkp/machine_name
    machine_path = config["bucket"]+"/bkp/"+platform.node()

    # get the backed up files for this machine
    backedup = get_backedup_files(machine_path, config, verbose)
    
    backedup_list = [ item for item in backedup.items() ]
    for lpath, dates in backedup_list:
        for d in dates:
            print("%s %s"%(time.ctime(d),lpath))

    return 0

def compact( config, dryrun = False, verbose = False):
    """ loop over all backups and remove empty ones compacting the s3 forest to only be backups with changed files """
    base_path = config["bucket"]+"/bkp/"

    machines = get_machines(base_path,config)
    for m in machines:
        machine_path = base_path+m
        backups = get_backups(machine_path, config, verbose)
        for b in backups:
            backup_path = machine_path+"/"+b.timestamp+"/"
            if verbose:
                print("Checking backup path: ",backup_path, file=sys.stderr)

            ls_output = io.StringIO(fs_mod.fs_ls(backup_path,False,lambda: config))
            empty = True
            for l in ls_output:
                m = re.match("(\s*DIR\s*)(\S*)$",l)
                if m:
                    if verbose:
                        print("Found directory: ",m.group(2), file=sys.stderr)
                    if not m.group(2).endswith("/bkp/"):
                        empty = False
            if empty:
                if not dryrun:
                    fs_mod.fs_del(backup_path,True,lambda: config)
                if verbose:
                    print("Removed empty backup: ",backup_path, file=sys.stderr)
            else:
                if verbose:
                    print("Skipped removing non-empty backup: ",backup_path, file=sys.stderr)

    return 0

def check_interrupted( verbose, config ):
    """ check for interrupted backups and send e-mail to error e-mail """
    message = ""
    for (dirpath, dirnames, filenames) in os.walk(os.path.expanduser("~/.bkp")):
        for f in filenames:
            if re.match("bkp\.[0-9][0-9][0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]\.log", f):
                message = message + f + "\n"
    if message:
        mail_error("Aborted backups found you may want to restart them!\n"+message, None, verbose, lambda: config )


class BackupJob:
    def __init__( self, config ):
        self.dryrun = False
        self.verbose = False
        self.init(config)

    def init( self, config ):
        """ reset or initialize the internal state for another job run """
        self.config = config
        self.start_time = 0.0
        self.end_time = 0.0
        self.machine_path = ""
        self.backup_path = ""
        self.remote_log_name = ""
        self.local_log_name = ""
        self.errors_count = 0
        self.worker_thread_pool = []
        self.work_queue = queue.Queue()
        self.worker_stop = False
        self.processed_files = {}
        self.backedup_files = {}
        self.logger = Logger()

    def get_config( self ):
        """ get the config for this backup job """
        return self.config

    def perform_logging( self ):
        """ perform the logging task loop reading the logging queue and write messages to output log file """
        start_time = time.time()
        while not self.logger.stopped():
            try:
                line = self.logger.get()
                if line:
                    try:
                        print(line, file=open(self.local_log_name,"a+"))
                        if self.verbose:
                            print(line, file=sys.stderr)
                    except:
                        print("Invalid Log Line!", file=sys.stderr)
                try:
                    # every 5 minutes checkpoint the log file to the server for safe keeping
                    if time.time() - start_time > 300:
                        start_time = time.time()
                        if not self.dryrun:
                            fs_mod.fs_put(self.local_log_name,self.remote_log_name,lambda: self.config, verbose=self.verbose)
                except:
                    print("Error checkpointing log file!", file=sys.stderr)
            except:
                print("Exception while logging!", file=sys.stderr)
                continue

    def set_dryrun( self, dr ):
        """ set the dryrun flag to true to prevent real actions in s3 """
        self.dryrun = dr

    def set_verbose( self, vb ):
        """ set the verbose flag to true to enable extended output """
        self.verbose = vb

    def load_processed( self, restart_file):
        """ load the processed files from a restart file """
        r = open(restart_file,"r")
        past_config = False
        for l in r:
            if past_config:
                local_path,remote_path,status,msg = l.split(";",3)
                self.processed_files[local_path] = (remote_path,status,msg)
            elif l.startswith("end_config"):
                past_config = True


    def log_success( self, from_path, to_path ):
        """ write a log line that indicates the copy of a source path to a destination s3 path, columns are from, to, "transferred", and "na" because there was no error """
        self.logger.log("%s;%s;transferred;na"%(from_path,to_path))

    def log_error( self, from_path, to_path, tb ):
        """ write a log line that indicates the copy of a source path to a destination s3 path, columns are from, to, "transferred", and "na" because there was no error """
        self.logger.log("%s;%s;error;%s"%(from_path,to_path,tb.replace("\n","/")))
        self.errors_count = self.errors_count + 1

    def process_backup( self ):
        """ thread body for worker thread, loop processing the queue until killed """

        start_time = time.time()
        while not self.worker_stop:
            try:
                # every 5 minutes dump a stack trace if self.verbose
                if time.time() - start_time > 300:
                    start_time = time.time()
                params = self.work_queue.get(True,1)
                try:
                    if not self.dryrun:
                        fs_mod.fs_put( params.from_path, params.to_path, lambda: self.config, verbose=self.verbose )
                    self.log_success( params.from_path, params.to_path )
                    self.work_queue.task_done()
                except:
                    tb = traceback.format_exc()
                    print(tb, file=sys.stderr)
                    self.log_error( params.from_path, params.to_path, tb )
                    self.work_queue.task_done()
            except queue.Empty:
                continue
            except:
                self.work_queue.task_done()
                tb = traceback.format_exc()
                print(tb, file=sys.stderr)
                continue

    def start_workers( self ):
        """ start the workers in the pool """
        num_threads = int(self.config["threads"])

        while num_threads:
            t = threading.Thread(target=self.process_backup)
            t.start()
            self.worker_thread_pool.append(t)
            num_threads = num_threads - 1

    def stop_workers( self ):
        """ stop the workers """
        self.worker_stop = True

    def wait_for_workers( self ):
        """ wait for the worker queue to be empty """
        if self.verbose:
            print("waiting for workers to finish", file=sys.stderr)
        if not self.work_queue.empty():
            self.work_queue.join()
        self.stop_workers()
        for t in self.worker_thread_pool:
            if t.is_alive():
                t.join()
        self.worker_thread_pool = []
        self.worker_stop = False
        self.work_queue = queue.Queue()
        if self.verbose:
            print("workers are done", file=sys.stderr)

    def backup_directory( self, path ):
        """ enqueue the files to be backed up for a given directory path, apply filters on datetime, pattern, non-hidden files only, recurse visible subdirs """
        for (dirpath, dirnames, filenames) in os.walk(path):
            if self.verbose:
                print("Scanning dirpath=",dirpath, file=sys.stderr)
            # if exclude_dirs is contained in any of the paths then return
            exclude_dir = False
            for e in self.config["exclude_dirs"]:
                if e and re.search(e,dirpath):
                    exclude_dir = True
                    break
            if exclude_dir:
                if self.verbose:
                    print("Excluding dirpath=",dirpath,"because of e=",e, file=sys.stderr)
                continue

            # get rid of hidden directories
            while True:
                deleted = False
                didx = 0
                for d in dirnames:
                    if d[0] == ".":
                        if self.verbose:
                            print("Deleting hidden directory =",d, file=sys.stderr)
                        del dirnames[didx]
                        deleted = True
                        break
                    didx = didx + 1
                if not deleted:
                    break

            # process files in the directory enqueueing included files for backup
            for f in filenames:
                # if it is a hidden file skip it
                if f[0] == ".":
                    if self.verbose:
                        print("Skipping hidden file =",f, file=sys.stderr)
                    continue

                # if it is excluded file skip it
                if self.config["exclude_files"] and re.match(self.config["exclude_files"],f):
                    if self.verbose:
                        print("Excluding file =",f,"Because of pattern=",self.config["exclude_files"], file=sys.stderr)
                    continue

                # build the absolute path for the file and it's backup path
                local_path = os.path.join(os.path.abspath(dirpath),f)
                remote_path = self.backup_path + urllib.request.pathname2url(local_path)

                # make sure local_path isn't in self.processed_files
                if local_path in self.processed_files:
                    if self.verbose:
                        print("Excluding file = ",local_path,"Because in processed_files", file=sys.stderr)
                    continue

                # if the file is in the time range for this backup then queue it for backup
                s = os.lstat(local_path)
                if (s.st_mtime >= self.start_time and s.st_mtime < self.end_time):
                    if self.verbose:
                        print("Enqueuing copy work",local_path,remote_path, file=sys.stderr)
                    self.work_queue.put(WorkerParams( local_path, remote_path ))
                elif not (local_path in self.backedup_files):
                    if self.verbose:
                        print("Enqueuing copy work because not in backup",local_path,remote_path, file=sys.stderr)
                    self.work_queue.put(WorkerParams( local_path, remote_path ))
                else:
                    if self.verbose:
                        print("Not Enqueuing copy work for ", local_path, "because time is out of range and it is backed up", file=sys.stderr)

        return

    def backup( self ):
        """ driver to perform backup """

        try:
            # reset our internal state for another run of backup
            self.init(self.config)

            # check for any aborted backups and send an e-mail about them
            check_interrupted(self.verbose,self.config)

            # the backups for a given machine will be in s3://bucket/bkp/machine_name
            self.machine_path = self.config["bucket"]+"/bkp/"+platform.node()

            # get the backed up files for this machine
            self.backedup_files = get_backedup_files(self.machine_path,self.config,self.verbose)

            # the start time for the next backup is in the "next" file in the root for that machine
            # if it is empty or doesn't exist then we start from the beginning of time
            # first thing we do is write the current time to the "next" file for the next backup
            # even if two backups are running concurrently they shouldn't interfere since the files shouldn't overlap
            next = get_contents( self.machine_path, "next", self.verbose, lambda: self.config )
            if next:
                self.start_time = float(next)
            else:
                self.start_time = 0.0
            self.end_time = time.time()
            put_contents( self.machine_path, "next", self.end_time, self.dryrun, lambda: self.config, self.verbose )
            end_time_t = time.localtime(self.end_time)
            self.config["start_time"] = str(self.start_time)
            self.config["end_time"] = str(self.end_time)

            # the backup root path is  s3://bucket/bkp/machine_name/datetime
            timestamp = "%04d.%02d.%02d.%02d.%02d.%02d"%(end_time_t.tm_year, end_time_t.tm_mon, end_time_t.tm_mday, end_time_t.tm_hour, end_time_t.tm_min, end_time_t.tm_sec)
            self.backup_path = self.machine_path + "/" + timestamp

            # we log locally and snapshot the log to a remote version in the backup
            # directory
            self.remote_log_name = self.backup_path + "/bkp/bkp."+ timestamp + ".log"
            self.local_log_name = os.path.expanduser("~/.bkp/bkp."+timestamp+".log")

            # write config and restart info to the start of the local log
            bkp_conf.save_config(self.config,open(self.local_log_name,"a+"),True)

            # start the logger thread
            self.logger.start_logger( self.perform_logging )

            # fire up the worker threads
            self.start_workers()

            # loop over the paths provided and add them to the work queue
            for d in self.config["dirs"]:
                self.backup_directory( d )

            # wait for queue to empty
            self.wait_for_workers()

            # wait for the logger to finish
            self.logger.wait_for_logger()

            # snapshot the log
            if not self.dryrun:
                fs_mod.fs_put(self.local_log_name,self.remote_log_name,lambda: self.config, verbose=self.verbose)
        except:
            self.stop_workers()
            self.logger.stop_logger()
            raise

        # send the log to the logging e-mail
        if self.errors_count:
            mail_error( None, open(self.local_log_name,"r"), self.verbose, lambda: self.config )
            os.remove(self.local_log_name)
            return 1
        else:
            mail_log( None, open(self.local_log_name,"r"), False, self.verbose, lambda: self.config )
            os.remove(self.local_log_name)
            return 0

    def restart( self, restart_file ):
        """ restart a previously aborted backup from a backup log file """

        try:
            # load the saved config from the log file
            # restore the original start and end time
            self.config = bkp_conf.config(restart_file, self.verbose)

            # initialize our internal state for this run
            self.init(self.config)

            # the backups for a given machine will be in s3://bucket/bkp/machine_name
            self.machine_path = self.config["bucket"]+"/bkp/"+platform.node()

            # get the backed up files for this machine
            self.backedup_files = get_backedup_files(self.machine_path,self.config,self.verbose)

            self.start_time = float(self.config["start_time"])
            self.end_time = float(self.config["end_time"])
            end_time_t = time.localtime(self.end_time)

            # load processed files into the filter
            self.load_processed(restart_file)

            # the backup root path is  s3://bucket/bkp/machine_name/datetime
            timestamp = "%04d.%02d.%02d.%02d.%02d.%02d"%(end_time_t.tm_year, end_time_t.tm_mon, end_time_t.tm_mday, end_time_t.tm_hour, end_time_t.tm_min, end_time_t.tm_sec)
            self.backup_path = self.machine_path + "/" + timestamp

            # we log locally and snapshot the log to a remote version in the backup
            # directory
            self.remote_log_name = self.backup_path + "/bkp/bkp."+ timestamp + ".log"
            self.local_log_name = os.path.expanduser("~/.bkp/bkp."+timestamp+".log")

            # start the logger thread
            self.logger.start_logger( self.perform_logging )

            # fire up the worker threads
            self.start_workers()

            # loop over the paths provided and add them to the work queue
            for d in self.config["dirs"]:
                self.backup_directory( d )

            # wait for queue to empty
            self.wait_for_workers()

            # wait for the logger to finish
            self.logger.wait_for_logger()

            # snapshot the log
            if not self.dryrun:
                fs_mod.fs_put(self.local_log_name,self.remote_log_name,lambda: self.config, verbose=self.verbose)
        finally:
            self.stop_workers()
            self.logger.stop_logger()

        if self.verbose:
            print("Exiting backup", file=sys.stderr)

        # send the log to the logging e-mail
        if self.errors_count:
            mail_error( None, open(self.local_log_name,"r"), self.verbose, lambda: self.config )
            os.remove(self.local_log_name)
            return 1
        else:
            mail_log( None, open(self.local_log_name,"r"), False, self.verbose, lambda: self.config )
            os.remove(self.local_log_name)
            return 0
