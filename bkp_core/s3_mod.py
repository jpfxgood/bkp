# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
""" module to implement shared functions for amazon s3 for the bkp/rstr tool """
import subprocess
import socket
import re
from io import StringIO

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

def s3_stat( remote_path ):
    """ return the modified time and size of an object at remote_path mtime resolution is seconds """
    cmd = "s3cmd info \"%s\""%(remote_path)
    p = subprocess.Popen(cmd,
                   shell=True,
                   bufsize=1024,
                   encoding="utf-8",
                   stdout=subprocess.PIPE,
                   stderr=subprocess.STDOUT)
    output = p.stdout.read()
    result = p.wait()
    if result:
        if output == "ERROR: S3 error: 404 (Not Found)\n":
            return (-1,-1)
        else:
            raise Exception(cmd,output,result)

    size = -1
    mtime = -1
    for l in StringIO(output):
        attr,value = l.strip().split(":",1)
        if attr == "File size":
            size = int(value)
        if attr == 'x-amz-meta-s3cmd-attrs':
            parts = value.split('/')
            meta = {}
            for p in parts:
                key,value = p.split(":",1)
                meta[key] = value
            mtime = int(meta["mtime"])
    return (mtime,size)
