# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
""" module to implement shared functions for amazon s3 for the bkp/rstr tool """
import subprocess

def s3_get( remote_path, local_path ):
    """ use s3cmd to copy a file to the local machine """
    cmd = "s3cmd -p -e -f get \"%s\" \"%s\""%(remote_path, local_path)
    p = subprocess.Popen(cmd,
                   shell=True,
                   bufsize=1024,
                   stdout=subprocess.PIPE,
                   stderr=subprocess.STDOUT)
    output = p.stdout.read()
    result = p.wait()
    if result:
        raise Exception(cmd,output,result)
    return
    
def s3_put( local_path, remote_path ):
    """ use s3cmd to copy a file from local machine to s3 """
    cmd = "s3cmd -p -e put \"%s\" \"%s\""%(local_path, remote_path)
    p = subprocess.Popen(cmd,
                   shell=True,
                   bufsize=1024,
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
                   stdout=subprocess.PIPE,
                   stderr=subprocess.STDOUT)
    output = p.stdout.read()
    result = p.wait()
    if result:
        raise Exception(cmd,output,result)
    return output
