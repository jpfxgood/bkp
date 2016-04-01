# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
""" module to implement shared config functions for the bkp tool """
import os
import sys

bkp_config = {}

def get_config():
    global bkp_config
    return bkp_config

def save_config( config_file, for_restart = False ):
    """ save the configuration to the file object passed as a parameter """
    print >>config_file, "bucket =", bkp_config["bucket"]
    print >>config_file, "dirs = ", ";".join(bkp_config["dirs"])
    print >>config_file, "exclude_files = ", bkp_config["exclude_files"]
    print >>config_file, "exclude_dirs = ",";".join(bkp_config["exclude_dirs"])
    print >>config_file, "log_email = ",bkp_config["log_email"]
    print >>config_file, "error_email = ",bkp_config["error_email"]
    print >>config_file, "threads = ",bkp_config["threads"]
    print >>config_file, "smtp_server = ",bkp_config["smtp_server"]
    print >>config_file, "smtp_username = ",bkp_config["smtp_username"]
    print >>config_file, "smtp_password = ",bkp_config["smtp_password"]
    print >>config_file, "ssh_username = ",bkp_config["ssh_username"]
    print >>config_file, "ssh_password = ",bkp_config["ssh_password"]
    if for_restart:
        print >>config_file, "start_time = ",bkp_config["start_time"]
        print >>config_file, "end_time = ",bkp_config["end_time"]
    print >>config_file, "end_config = True"
    return 0
    
def configure ():
    """ prompt for configuration parameters to build initial ~/.bkp/bkp_config """
    bkp_config["bucket"] = raw_input("Enter the name of your Amazon S3 bucket, file path, or ssh path:")
    bkp_config["dirs"] = raw_input("Enter a semicolon (;) delimited list of directories to backup (will include subdirectories):").split(";")
    bkp_config["exclude_files"] = raw_input("Enter a python regular expression to exclude matching file names:")
    bkp_config["exclude_dirs"] = raw_input("Enter a semicolon (;) delimited list of directories to exclude (including subdirectories):").split(";")
    bkp_config["log_email"] = raw_input("Enter an e-mail address to send log files to:")
    bkp_config["error_email"] = raw_input("Enter an e-mail address to send errors to:")
    bkp_config["smtp_server"] = raw_input("Enter your smtp server name:")
    bkp_config["smtp_username"] = raw_input("Enter your smtp user name:")
    bkp_config["smtp_password"] = raw_input("Enter your smtp password:")
    bkp_config["ssh_username"] = raw_input("Enter your ssh user name:")
    bkp_config["ssh_password"] = raw_input("Enter your ssh password:")
    bkp_config["threads"] = raw_input("Enter the number of threads to use for transfers:")
                
    bkp_dir = os.path.expanduser("~/.bkp")
    if not os.path.exists(bkp_dir):
        os.mkdir(bkp_dir)
    save_config(open(os.path.join(bkp_dir,"bkp_config"), "w"))
    return 0                    

def config( config_file, verbose = False ):
    """ load configuration for a backup from a config file """
    global bkp_config
    config_path = os.path.expanduser(config_file)
    for l in open(config_path,"r"):
        l = l.strip()
        if l :
            key, value = l.split("=",1)
            key = key.strip().lower() 
            if key == "end_config":
                break
            value = value.strip()
            if key in ["dirs","exclude_dirs"]:
                value = [f.strip() for f in value.split(";")]
            if verbose:
                print >>sys.stderr,"config key =",key,"value =", value
            bkp_config[key] = value
            
    bucket = bkp_config["bucket"]
    if not (bucket.startswith("ssh://") or bucket.startswith("file://") or bucket.startswith("s3://")):
        bkp_config["bucket"] = "s3://"+bucket
    return
