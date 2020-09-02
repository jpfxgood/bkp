# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
""" module to implement shared functions for amazon s3 for the bkp/rstr tool """
import subprocess
import socket
import re
from io import StringIO
import copy
import os
import sys
import time
import traceback
from datetime import datetime

def s3_get( remote_path, local_path ):
    """ use s3cmd to copy a file to the local machine """
    cmd = "s3cmd -p --no-encrypt -f get \"%s\" \"%s\""%(remote_path, local_path)
    p = subprocess.Popen(cmd,
                   shell=True,
                   bufsize=1024,
                   encoding="utf-8",
                   stdout=subprocess.PIPE,
                   stderr=subprocess.STDOUT)
    output = p.stdout.read()
    result = p.wait()
    if result:
        raise Exception(cmd,output,result)
    return

def s3_put( local_path, remote_path ):
    """ use s3cmd to copy a file from local machine to s3 """
    cmd = "s3cmd -p --no-encrypt put \"%s\" \"%s\""%(local_path, remote_path)
    p = subprocess.Popen(cmd,
                   shell=True,
                   bufsize=1024,
                   encoding="utf-8",
                   stdout=subprocess.PIPE,
                   stderr=subprocess.STDOUT)
    output = p.stdout.read()
    result = p.wait()
    if result:
        raise Exception(cmd,output,result)
    return

def s3_ls( path, recurse=False ):
    """ perform an ls of the s3 path specified and return the output """
    r_opt = ""
    if recurse:
        r_opt = "-r"
    cmd = "s3cmd %s ls \"%s\""%(r_opt, path)
    p = subprocess.Popen(cmd,
                   shell=True,
                   bufsize=1024,
                   encoding="utf-8",
                   stdout=subprocess.PIPE,
                   stderr=subprocess.STDOUT)
    output = p.stdout.read()
    result = p.wait()
    if result:
        raise Exception(cmd,output,result)
    dirs = []
    files = []
    final_output = StringIO()
    for l in StringIO(output):
        l = l.strip()
        parts = re.split(r'\s+',l,3)
        if parts[0] == "DIR":
            dirs.append(parts[-1])
        else:
            files.append(parts[-1])
    for d in dirs:
        print("                           DIR %s"%(d), file=final_output)
    metas = s3_info(files)
    for m in metas:
        if 'error' in m:
            raise Exception(cmd,output,result)
        if 'mtime' in m:
            mtime = datetime.fromtimestamp(float(m['mtime']))
        else:
            last_mod = datetime.strptime(m['Last mod'],"%a, %d %b %Y %H:%M:%S %Z")
            last_mod = last_mod.astimezone(tz=None)
            mtime = last_mod
        size = int(m['File size'])
        print("%04d-%02d-%02d %02d:%02d %9d   %s"%(mtime.year,mtime.month,mtime.day,mtime.hour,mtime.minute,size,m["s3_object_name"]), file=final_output)

    output = final_output.getvalue()
    final_output.close()
    return output

def s3_del( path, recurse=False ):
    """ perform an del of the s3 path specified and return the output """
    r_opt = ""
    if recurse:
        r_opt = "-r"
    cmd = "s3cmd %s del \"%s\""%(r_opt, path)
    p = subprocess.Popen(cmd,
                   shell=True,
                   bufsize=1024,
                   encoding="utf-8",
                   stdout=subprocess.PIPE,
                   stderr=subprocess.STDOUT)
    output = p.stdout.read()
    result = p.wait()
    if result:
        raise Exception(cmd,output,result)
    return output

def s3_test( remote_path, verbose = False ):
    """ test to make sure that we can access the remote path """

    try:
        host, port, path = ("s3.amazonaws.com",80,"")

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host,port))
        data = s.recv(1024)
        if verbose:
            print("s3_test: ", data, file=sys.stderr)
        s.close()
        return True
    except:
        if verbose:
            print(traceback.format_exc(), file=sys.stderr)
        return False

def s3_info( remote_path ):
    """ remote_path can be either a single full path name or a list of path names a single path name will return a map of the metadata for the object, a list will return a list of metadatas one per object """
    paths = []
    is_list = False
    if 'SC_ARG_MAX' in os.sysconf_names:
        arg_max = os.sysconf('SC_ARG_MAX')
    else:
        arg_max = 8191

    if isinstance(remote_path,str):
        paths.append(remote_path)
    elif isinstance(remote_path,list):
        paths = copy.copy(remote_path)
        is_list = True

    metas = []
    cmd_paths = []
    cmd_base = "s3cmd info "
    cmd = cmd_base

    def run_cmd():
        p = subprocess.Popen(cmd,
                       shell=True,
                       bufsize=1024,
                       encoding="utf-8",
                       stdout=subprocess.PIPE,
                       stderr=subprocess.STDOUT)
        output = p.stdout.read()
        result = p.wait()

        meta = {}
        cmd_idx = 0
        for l in StringIO(output):
            l = l.strip()
            if l.startswith("s3://") and l.endswith(" (object):"):
                if meta:
                    metas.append(meta)
                    cmd_idx += 1
                meta = { "s3_object_name":l[:-len(" (object):")] }
            elif l.startswith("s3://") and l.endswith(" (bucket):"):
                if meta:
                    metas.append(meta)
                    cmd_idx += 1
                meta = { "s3_bucket_name":l[:-len(" (bucket):")] }
            elif l.startswith("ERROR:"):
                if l == "ERROR: S3 error: 404 (Not Found)":
                    meta = { "s3_object_name":cmd_paths[cmd_idx], "mtime": "-1", "File size": "-1", "error" : l }
                    metas.append(meta)
                    cmd_idx += 1
                    continue
                else:
                    raise Exception(cmd,output,result)
            else:
                name, value = l.split(":",1)
                name = name.strip()
                value = value.strip()
                if name == 'x-amz-meta-s3cmd-attrs':
                    parts = value.split('/')
                    for p in parts:
                        key,value = p.split(":",1)
                        meta[key] = value
                elif name in meta:
                    if isinstance(meta[name],list):
                        meta[name].append(value)
                    else:
                        meta[name] = [ meta[name], value ]
                else:
                    meta[name] = value
        if meta:
            metas.append(meta)

    while paths:
        r_path = paths.pop(0)
        if len(cmd+' "%s"'%r_path) > arg_max:
            paths.insert(0,r_path)
            run_cmd()
            cmd = cmd_base
            cmd_paths = []
        else:
            cmd += ' "%s"'%r_path
            cmd_paths.append(r_path)

        if not paths:
            run_cmd()
            cmd_paths = []

    if is_list:
        return metas
    else:
        return metas[0]

def s3_stat( remote_path ):
    """ return the modified time and size of an object at remote_path mtime resolution is seconds """
    meta = s3_info(remote_path)
    return (int(meta["mtime"]),int(meta["File size"]))
