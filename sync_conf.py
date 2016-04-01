# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
""" module to implement shared config functions for the sync tool """
import os
import sys

sync_config = {}

def get_config():
    global sync_config
    return sync_config

def save_config( config_file ):
    """ save the configuration to the file object passed as a parameter """
    print >>config_file, "target =", sync_config["target"]
    print >>config_file, "dirs = ", ";".join(sync_config["dirs"])
    print >>config_file, "exclude_files = ", sync_config["exclude_files"]
    print >>config_file, "exclude_dirs = ",";".join(sync_config["exclude_dirs"])
    print >>config_file, "threads = ",sync_config["threads"]
    print >>config_file, "ssh_username = ",sync_config["ssh_username"]
    print >>config_file, "ssh_password = ",sync_config["ssh_password"]
    print >>config_file, "end_config = True"
    return 0
    
def configure ():
    """ prompt for configuration parameters to build initial ~/.sync/sync_config """
    sync_config["target"] = raw_input("Enter the name of the ssh path to synchronize with:")
    sync_config["dirs"] = raw_input("Enter a semicolon (;) delimited list of directories to backup (will include subdirectories):").split(";")
    sync_config["exclude_files"] = raw_input("Enter a python regular expression to exclude matching file names:")
    sync_config["exclude_dirs"] = raw_input("Enter a semicolon (;) delimited list of directories to exclude (including subdirectories):").split(";")
    sync_config["ssh_username"] = raw_input("Enter your ssh user name:")
    sync_config["ssh_password"] = raw_input("Enter your ssh password:")
    sync_config["threads"] = raw_input("Enter the number of threads to use for transfers:")
                
    sync_dir = os.path.expanduser("~/.sync")
    if not os.path.exists(sync_dir):
        os.mkdir(sync_dir)
    save_config(open(os.path.join(sync_dir,"sync_config"), "w"))
    return 0                    

def config( config_file, verbose ):
    """ load configuration for a backup from a config file """
    global sync_config
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
            sync_config[key] = value
    return sync_config
