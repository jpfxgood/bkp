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
from bkp_core import logger
from bkp_core.logger import start_logger, stop_logger, wait_for_logger, log, get, stopped

dryrun = False
verbose = False
work_queue = queue.Queue()
start_time = 0.0
end_time = 0.0
machine_path = ""
backup_path = ""
remote_log_name = ""
local_log_name = ""
errors_count = 0
worker_thread_pool = []
processed_files = {}
worker_stop = False
backedup_files = {}

def perform_logging():
    """ perform the logging task loop reading the logging queue and write messages to output log file """
    start_time = time.time()
    while not stopped():
        try:
            line = get()
            if line:
                try:
                    print(line, file=open(local_log_name,"a+"))
                    if verbose:
                        print(line, file=sys.stderr)
                except:
                    print("Invalid Log Line!", file=sys.stderr)
            try:
                # every 5 minutes checkpoint the log file to the server for safe keeping
                if time.time() - start_time > 300:
                    start_time = time.time()
                    if not dryrun:
                        fs_mod.fs_put(local_log_name,remote_log_name,verbose=verbose)
            except:
                print("Error checkpointing log file!", file=sys.stderr)
        except:
            print("Exception while logging!", file=sys.stderr)
            continue

def set_dryrun( dr ):
    """ set the dryrun flag to true to prevent real actions in s3 """
    global dryrun
    dryrun = dr

def set_verbose( vb ):
    """ set the verbose flag to true to enable extended output """
    global verbose
    verbose = vb


def load_processed(restart_file):
    """ load the processed files from a restart file """
    global processed_files
    r = open(restart_file,"r")
    past_config = False
    for l in r:
        if past_config:
            local_path,remote_path,status,msg = l.split(";",3)
            processed_files[local_path] = (remote_path,status,msg)
        elif l.startswith("end_config"):
            past_config = True

def restart( restart_file ):
    """ restart a previously aborted backup from a backup log file """
    global backedup_files, machine_path, start_time, end_time, backup_path, remote_log_name, local_log_name, errors_count

    try:
        # load the saved config from the log file
        # restore the original start and end time
        bkp_conf.config(restart_file, verbose)

        # the backups for a given machine will be in s3://bucket/bkp/machine_name
        machine_path = bkp_conf.get_config()["bucket"]+"/bkp/"+platform.node()

        # get the backed up files for this machine
        backedup_files = get_backedup_files(machine_path)

        start_time = float(bkp_conf.get_config()["start_time"])
        end_time = float(bkp_conf.get_config()["end_time"])
        end_time_t = time.localtime(end_time)

        # load processed files into the filter
        load_processed(restart_file)

        # the backup root path is  s3://bucket/bkp/machine_name/datetime
        timestamp = "%04d.%02d.%02d.%02d.%02d.%02d"%(end_time_t.tm_year, end_time_t.tm_mon, end_time_t.tm_mday, end_time_t.tm_hour, end_time_t.tm_min, end_time_t.tm_sec)
        backup_path = machine_path + "/" + timestamp

        # we log locally and snapshot the log to a remote version in the backup
        # directory
        remote_log_name = backup_path + "/bkp/bkp."+ timestamp + ".log"
        local_log_name = os.path.expanduser("~/.bkp/bkp."+timestamp+".log")

        # start the logger thread
        start_logger( perform_logging )

        # fire up the worker threads
        start_workers()

        # loop over the paths provided and add them to the work queue
        for d in bkp_conf.get_config()["dirs"]:
            backup_directory( d )

        # wait for queue to empty
        wait_for_workers()

        # wait for the logger to finish
        wait_for_logger()

        # snapshot the log
        if not dryrun:
            fs_mod.fs_put(local_log_name,remote_log_name,verbose=verbose)
    finally:
        stop_workers()
        stop_logger()

    if verbose:
        print("Exiting backup", file=sys.stderr)

    # send the log to the logging e-mail
    if errors_count:
        mail_error( None, open(local_log_name,"r"), verbose )
        os.remove(local_log_name)
        return 1
    else:
        mail_log( None, open(local_log_name,"r"), False, verbose )
        os.remove(local_log_name)
        return 0

class WorkerParams:
    """ worker params """
    def __init__(self, from_path, to_path):
        """ set up the copy from and to paths for the worker """
        self.from_path = from_path
        self.to_path = to_path

def log_success( from_path, to_path ):
    """ write a log line that indicates the copy of a source path to a destination s3 path, columns are from, to, "transferred", and "na" because there was no error """
    log("%s;%s;transferred;na"%(from_path,to_path))

def log_error( from_path, to_path, tb ):
    """ write a log line that indicates the copy of a source path to a destination s3 path, columns are from, to, "transferred", and "na" because there was no error """
    global errors_count
    log("%s;%s;error;%s"%(from_path,to_path,tb.replace("\n","/")))
    errors_count = errors_count + 1

def process_backup():
    """ thread body for worker thread, loop processing the queue until killed """
    start_time = time.time()
    while not worker_stop:
        try:
            # every 5 minutes dump a stack trace if verbose
            if time.time() - start_time > 300:
                start_time = time.time()
            params = work_queue.get(True,1)
            try:
                if not dryrun:
                    fs_mod.fs_put( params.from_path, params.to_path, verbose=verbose )
                log_success( params.from_path, params.to_path )
                work_queue.task_done()
            except:
                tb = traceback.format_exc()
                print(tb, file=sys.stderr)
                log_error( params.from_path, params.to_path, tb )
                work_queue.task_done()
        except queue.Empty:
            continue
        except:
            work_queue.task_done()
            tb = traceback.format_exc()
            print(tb, file=sys.stderr)
            continue


def start_workers():
    """ start the workers in the pool """
    global worker_thread_pool
    num_threads = int(bkp_conf.get_config()["threads"])

    while num_threads:
        t = threading.Thread(target=process_backup)
        t.start()
        worker_thread_pool.append(t)
        num_threads = num_threads - 1


def stop_workers():
    """ stop the workers """
    global worker_stop
    worker_stop = True

def wait_for_workers():
    """ wait for the worker queue to be empty """
    if verbose:
        print("waiting for workers to finish", file=sys.stderr)
    if not work_queue.empty():
        work_queue.join()
    stop_workers()
    for t in worker_thread_pool:
        if t.is_alive():
            t.join()
    if verbose:
        print("workers are done", file=sys.stderr)

def check_interrupted():
    """ check for interrupted backups and send e-mail to error e-mail """
    message = ""
    for (dirpath, dirnames, filenames) in os.walk(os.path.expanduser("~/.bkp")):
        for f in filenames:
            if re.match("bkp\.[0-9][0-9][0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]\.[0-9][0-9]\.log", f):
                message = message + f + "\n"
    if message:
        mail_error("Aborted backups found you may want to restart them!\n"+message, None, verbose)

def backup_directory( path ):
    """ enqueue the files to be backed up for a given directory path, apply filters on datetime, pattern, non-hidden files only, recurse visible subdirs """
    for (dirpath, dirnames, filenames) in os.walk(path):
        if verbose:
            print("Scanning dirpath=",dirpath, file=sys.stderr)
        # if exclude_dirs is contained in any of the paths then return
        exclude_dir = False
        for e in bkp_conf.get_config()["exclude_dirs"]:
            if e and re.search(e,dirpath):
                exclude_dir = True
                break
        if exclude_dir:
            if verbose:
                print("Excluding dirpath=",dirpath,"because of e=",e, file=sys.stderr)
            continue

        # get rid of hidden directories
        while True:
            deleted = False
            didx = 0
            for d in dirnames:
                if d[0] == ".":
                    if verbose:
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
                if verbose:
                    print("Skipping hidden file =",f, file=sys.stderr)
                continue

            # if it is excluded file skip it
            if bkp_conf.get_config()["exclude_files"] and re.match(bkp_conf.get_config()["exclude_files"],f):
                if verbose:
                    print("Excluding file =",f,"Because of pattern=",bkp_conf.get_config()["exclude_files"], file=sys.stderr)
                continue

            # build the absolute path for the file and it's backup path
            local_path = os.path.join(os.path.abspath(dirpath),f)
            remote_path = backup_path + urllib.request.pathname2url(local_path)

            # make sure local_path isn't in processed_files
            if local_path in processed_files:
                if verbose:
                    print("Excluding file = ",local_path,"Because in processed_files", file=sys.stderr)
                continue

            # if the file is in the time range for this backup then queue it for backup
            s = os.lstat(local_path)
            if (s.st_mtime >= start_time and s.st_mtime < end_time):
                if verbose:
                    print("Enqueuing copy work",local_path,remote_path, file=sys.stderr)
                work_queue.put(WorkerParams( local_path, remote_path ))
            elif not (local_path in backedup_files):
                if verbose:
                    print("Enqueuing copy work because not in backup",local_path,remote_path, file=sys.stderr)
                work_queue.put(WorkerParams( local_path, remote_path ))
            else:
                if verbose:
                    print("Not Enqueuing copy work for ", local_path, "because time is out of range and it is backed up", file=sys.stderr)

    return

def backup():
    """ driver to perform backup """
    global machine_path, start_time, end_time, backup_path, remote_log_name, local_log_name, errors_count, backedup_files

    try:
        # check for any aborted backups and send an e-mail about them
        check_interrupted()

        # the backups for a given machine will be in s3://bucket/bkp/machine_name
        machine_path = bkp_conf.get_config()["bucket"]+"/bkp/"+platform.node()

        # get the backed up files for this machine
        backedup_files = get_backedup_files(machine_path)

        # the start time for the next backup is in the "next" file in the root for that machine
        # if it is empty or doesn't exist then we start from the beginning of time
        # first thing we do is write the current time to the "next" file for the next backup
        # even if two backups are running concurrently they shouldn't interfere since the files shouldn't overlap
        next = get_contents( machine_path, "next", verbose)
        if next:
            start_time = float(next)
        else:
            start_time = 0.0
        end_time = time.time()
        put_contents( machine_path, "next", end_time, dryrun, bkp_conf.get_config, verbose )
        end_time_t = time.localtime(end_time)
        bkp_conf.get_config()["start_time"] = start_time
        bkp_conf.get_config()["end_time"] = end_time

        # the backup root path is  s3://bucket/bkp/machine_name/datetime
        timestamp = "%04d.%02d.%02d.%02d.%02d.%02d"%(end_time_t.tm_year, end_time_t.tm_mon, end_time_t.tm_mday, end_time_t.tm_hour, end_time_t.tm_min, end_time_t.tm_sec)
        backup_path = machine_path + "/" + timestamp

        # we log locally and snapshot the log to a remote version in the backup
        # directory
        remote_log_name = backup_path + "/bkp/bkp."+ timestamp + ".log"
        local_log_name = os.path.expanduser("~/.bkp/bkp."+timestamp+".log")

        # write config and restart info to the start of the local log
        bkp_conf.save_config(open(local_log_name,"a+"),True)

        # start the logger thread
        start_logger( perform_logging )

        # fire up the worker threads
        start_workers()

        # loop over the paths provided and add them to the work queue
        for d in bkp_conf.get_config()["dirs"]:
            backup_directory( d )

        # wait for queue to empty
        wait_for_workers()

        # wait for the logger to finish
        wait_for_logger()

        # snapshot the log
        if not dryrun:
            fs_mod.fs_put(local_log_name,remote_log_name, verbose=verbose)
    finally:
        stop_workers()
        stop_logger()

    # send the log to the logging e-mail
    if errors_count:
        mail_error( None, open(local_log_name,"r"), verbose )
        os.remove(local_log_name)
        return 1
    else:
        mail_log( None, open(local_log_name,"r"), False, verbose )
        os.remove(local_log_name)
        return 0


def get_machines(base_path):
    """ get all the machines that there are backups for """
    machines = []
    ls_output = io.StringIO(fs_mod.fs_ls(base_path))
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

def get_backups( machine_path ):
    """ get a list of all of the backups for this machine, returns list of Backup classes """
    # make sure the path ends in a / so we get the contents and not the directory itself
    if machine_path[-1] != '/':
        machine_path = machine_path + '/'

    # loop over the ls output and extract the paths and then parse out and evaluate the timestamps
    backups = []
    try:
        ls_output = io.StringIO(fs_mod.fs_ls(machine_path))
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

def get_backedup_files( machine_path ):
    """ return a dict with all of the files we've backed up for this machine """
    backedup = {}
    backups = get_backups( machine_path )
    for bk in backups:
        # fetch the contents of the backup log
        contents = get_contents(machine_path,bk.timestamp+"/bkp/bkp."+bk.timestamp+".log",verbose)

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
            for l in io.StringIO(fs_mod.fs_ls(bk.path,True)):
                prefix,path = re.split(bk.timestamp,l)
                path = path.strip()
                local_path = urllib.request.url2pathname(path)
                if local_path in backedup:
                    backedup[local_path].append(bk.time)
                else:
                    backedup[local_path] = [bk.time]

    return backedup

def list():
    """ generate a listing of all of the files backed up for this machine with the dates available """
    # the backups for a given machine will be in s3://bucket/bkp/machine_name
    machine_path = bkp_conf.get_config()["bucket"]+"/bkp/"+platform.node()

    # get the backed up files for this machine
    backedup = get_backedup_files(machine_path)

    for lpath, dates in list(backedup.items()):
        for d in dates:
            print("%s %s"%(time.ctime(d),lpath))

    return 0

def compact():
    """ loop over all backups and remove empty ones compacting the s3 forest to only be backups with changed files """
    base_path = bkp_conf.get_config()["bucket"]+"/bkp/"

    machines = get_machines(base_path)
    for m in machines:
        machine_path = base_path+m
        backups = get_backups(machine_path)
        for b in backups:
            backup_path = machine_path+"/"+b.timestamp+"/"
            if verbose:
                print("Checking backup path: ",backup_path, file=sys.stderr)

            ls_output = io.StringIO(fs_mod.fs_ls(backup_path))
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
                    fs_mod.fs_del(backup_path,True)
                if verbose:
                    print("Removed empty backup: ",backup_path, file=sys.stderr)
            else:
                if verbose:
                    print("Skipped removing non-empty backup: ",backup_path, file=sys.stderr)

    return 0
