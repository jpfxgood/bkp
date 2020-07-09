# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
""" module to implement shared config functions for the bkp tool """
import os
import sys

def save_config( bkp_config, config_file, for_restart = False ):
    """ save the configuration to the file object passed as a parameter """
    print("bucket =", bkp_config["bucket"], file=config_file)
    print("dirs = ", ";".join(bkp_config["dirs"]), file=config_file)
    print("exclude_files = ", bkp_config["exclude_files"], file=config_file)
    print("exclude_dirs = ",";".join(bkp_config["exclude_dirs"]), file=config_file)
    print("log_email = ",bkp_config["log_email"], file=config_file)
    print("error_email = ",bkp_config["error_email"], file=config_file)
    print("threads = ",bkp_config["threads"], file=config_file)
    print("ssh_username = ",bkp_config["ssh_username"], file=config_file)
    print("ssh_password = ",bkp_config["ssh_password"], file=config_file)
    if for_restart:
        print("start_time = ",bkp_config["start_time"], file=config_file)
        print("end_time = ",bkp_config["end_time"], file=config_file)
    print("end_config = True", file=config_file)
    return 0

def configure ():
    bkp_config = {}
    """ prompt for configuration parameters to build initial ~/.bkp/bkp_config """
    bkp_config["bucket"] = input("Enter the name of your Amazon S3 bucket, file path, or ssh path:")
    bkp_config["dirs"] = input("Enter a semicolon (;) delimited list of directories to backup (will include subdirectories):").split(";")
    bkp_config["exclude_files"] = input("Enter a python regular expression to exclude matching file names:")
    bkp_config["exclude_dirs"] = input("Enter a semicolon (;) delimited list of directories to exclude (including subdirectories):").split(";")
    bkp_config["log_email"] = input("Enter an e-mail address to send log files to:")
    bkp_config["error_email"] = input("Enter an e-mail address to send errors to:")
    bkp_config["ssh_username"] = input("Enter your ssh user name:")
    bkp_config["ssh_password"] = input("Enter your ssh password:")
    bkp_config["threads"] = input("Enter the number of threads to use for transfers:")

    bkp_dir = os.path.expanduser("~/.bkp")
    if not os.path.exists(bkp_dir):
        os.mkdir(bkp_dir)
    save_config(bkp_config,open(os.path.join(bkp_dir,"bkp_config"), "w"))
    return 0

def config( config_file, verbose = False ):
    """ load configuration for a backup from a config file """
    bkp_config = {}
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
                print("config key =",key,"value =", value, file=sys.stderr)
            bkp_config[key] = value

    bucket = bkp_config["bucket"]
    if not (bucket.startswith("ssh://") or bucket.startswith("file://") or bucket.startswith("s3://")):
        bkp_config["bucket"] = "s3://"+bucket
    return bkp_config
