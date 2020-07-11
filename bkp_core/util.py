# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
""" module to implement shared functions for the bkp tool """

from email.mime.text import MIMEText
from bkp_core import fs_mod
import sys
import os
import tempfile
import re
import traceback
import platform
import datetime
import subprocess
import time

def get_contents( path, name, verbose = False, get_config=lambda: {} ):
    """ fetch the contents of an s3 file and return it's contents as a string """
    t_file_fh, t_file_name = tempfile.mkstemp()
    os.close(t_file_fh)
    try:
        fs_mod.fs_get( path+"/"+name, t_file_name, get_config )
    except:
        if verbose:
            print("get_contents exception:",traceback.format_exc(), file=sys.stderr)
        return ""
    contents = open(t_file_name,"r").read()
    os.remove(t_file_name)
    return contents

def put_contents( path, name, contents, dryrun = False, get_config=lambda: {}, verbose=False ):
    """ put the contents string to the s3 file at path, name  """
    t_file_fh, t_file_name = tempfile.mkstemp()
    os.close(t_file_fh)
    print(contents, file=open(t_file_name,"w"))
    if not dryrun:
        fs_mod.fs_put( t_file_name, path+"/"+name, get_config, verbose )
        if not path.startswith("s3://"):
            t = time.time()
            fs_mod.fs_utime( path+"/"+name, (t,t), get_config )
    os.remove(t_file_name)
    return

def send_email( mime_msg ):
    """ given a mime message with From: To: Subject: headers """
    cmd = "/usr/sbin/ssmtp %s"%mime_msg['To']
    p = subprocess.Popen(cmd,
                        shell=True,
                        bufsize=1024,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        stdin=subprocess.PIPE,
                        universal_newlines=True)
    output = p.communicate(input=mime_msg.as_string())[0]
    result = p.wait()
    if result:
        raise Exception(cmd,output,result)
    return output

def mail_error( error, log_file=None, verbose = False, get_config=lambda: {} ):
    """ e-mail an error report to the error e-mail account """
    return mail_log( error, log_file, True, verbose, get_config=get_config )

def mail_log( log, log_file=None, is_error = False, verbose = False, get_config=lambda: {} ):
    """ e-mail a log file to the log e-mail account """
    tries = 3
    log_text = ""
    while tries:
        try:
            if log != None:
                msg = MIMEText(log)
            elif log_file != None:
                log_text = re.sub("^smtp_.*$|^ssh_.*$","",log_file.read(),flags=re.M)
                msg = MIMEText(log_text[:2*pow(2,20)])
            else:
                return 0

            if is_error:
                if verbose:
                    print("E-mailing log file with errors", file=sys.stderr)
                msg['Subject'] = "bkp error: %s "%(platform.node())
                msg['From'] = get_config()["error_email"]
                msg['To'] = get_config()["error_email"]
            else:
                if verbose:
                    print("E-mailing log file with no errors", file=sys.stderr)
                msg['Subject'] = "bkp complete: %s"%(platform.node())
                msg['From'] = get_config()["log_email"]
                msg['To'] = get_config()["log_email"]

            msg['Date'] = datetime.datetime.now().strftime( "%m/%d/%Y %H:%M" )
            send_email(msg)
            return 0
        except:
            time.sleep(tries*10.0)
            tries = tries - 1
            if not tries:
                if is_error:
                    print("Error couldn't send via e-mail", file=sys.stderr)
                else:
                    print("Success couldn't send via e-mail", file=sys.stderr)
                if log:
                    print(log, file=sys.stderr)
                if log_text:
                    print(log_text, file=sys.stderr)
                print(traceback.format_exc(), file=sys.stderr)
                raise
