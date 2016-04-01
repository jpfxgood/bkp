# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
""" module to implement shared functions for the bkp tool """

from email.mime.text import MIMEText
import fs_mod
import sys
import os
import tempfile
import re
import traceback
import bkp_conf
import platform
import datetime
import smtplib
import time

def get_contents( path, name, verbose = False, get_config=bkp_conf.get_config ):
    """ fetch the contents of an s3 file and return it's contents as a string """
    t_file_fh, t_file_name = tempfile.mkstemp()
    os.close(t_file_fh)
    try:
        fs_mod.fs_get( path+"/"+name, t_file_name, get_config=bkp_conf.get_config )
    except:     
        if verbose:
            print >>sys.stderr,"get_contents exception:",traceback.format_exc()
        return ""
    contents = open(t_file_name,"r").read()
    os.remove(t_file_name)
    return contents
    
def put_contents( path, name, contents, dryrun = False, get_config=bkp_conf.get_config, verbose=False ):
    """ put the contents string to the s3 file at path, name  """
    t_file_fh, t_file_name = tempfile.mkstemp()
    os.close(t_file_fh)
    print >>open(t_file_name,"w"), contents
    if not dryrun:
        fs_mod.fs_put( t_file_name, path+"/"+name, get_config, verbose )
        if not path.startswith("s3://"):    
            t = time.time()
            fs_mod.fs_utime( path+"/"+name, (t,t), get_config )
    os.remove(t_file_name)
    return
    
def mail_error( error, log_file=None, verbose = False, get_config=bkp_conf.get_config ):
    """ e-mail an error report to the error e-mail account """
    return mail_log( error, log_file, True, verbose, get_config=get_config )
    
def mail_log( log, log_file=None, is_error = False, verbose = False, get_config=bkp_conf.get_config ):
    """ e-mail a log file to the log e-mail account """
    tries = 3
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
                    print >>sys.stderr,"E-mailing log file with errors"
                msg['Subject'] = "bkp error: %s "%(platform.node())
                msg['From'] = get_config()["error_email"]
                msg['To'] = get_config()["error_email"]
            else:
                if verbose:
                    print >>sys.stderr,"E-mailing log file with no errors"
                msg['Subject'] = "bkp complete: %s"%(platform.node())
                msg['From'] = get_config()["log_email"]
                msg['To'] = get_config()["log_email"]
    
            msg['Date'] = datetime.datetime.now().strftime( "%m/%d/%Y %H:%M" )
    
            s = smtplib.SMTP_SSL(get_config()["smtp_server"])
            s.login(get_config()["smtp_username"],get_config()["smtp_password"])
            s.sendmail(msg['From'],msg['To'],msg.as_string())
            s.quit()
            return 0
        except:
            time.sleep(tries*10.0)
            tries = tries - 1
            if not tries:
                if is_error:
                    print >>sys.stderr, "Error couldn't send via e-mail"
                else:
                    print >>sys.stderr, "Success couldn't send via e-mail"
                if log:
                    print >>sys.stderr,log
                if log_text:
                    print >>sys.stderr,log_text
                print >>sys.stderr,traceback.format_exc()
                raise
