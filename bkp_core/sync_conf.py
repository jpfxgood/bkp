# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
""" module to implement shared config functions for the sync tool """
import os
import sys

def save_config( sync_config, config_file ):
    """ save the configuration to the file object passed as a parameter """
    print("target =", sync_config["target"], file=config_file)
    print("dirs = ", ";".join(sync_config["dirs"]), file=config_file)
    print("exclude_files = ", sync_config["exclude_files"], file=config_file)
    print("exclude_dirs = ",";".join(sync_config["exclude_dirs"]), file=config_file)
    print("threads = ",sync_config["threads"], file=config_file)
    print("ssh_username = ",sync_config["ssh_username"], file=config_file)
    print("ssh_password = ",sync_config["ssh_password"], file=config_file)
    print("end_config = True", file=config_file)
    return 0

def configure ():
    sync_config = {}
    """ prompt for configuration parameters to build initial ~/.sync/sync_config """
    sync_config["target"] = input("Enter the name of the ssh path to synchronize with:")
    sync_config["dirs"] = input("Enter a semicolon (;) delimited list of directories to backup (will include subdirectories):").split(";")
    sync_config["exclude_files"] = input("Enter a python regular expression to exclude matching file names:")
    sync_config["exclude_dirs"] = input("Enter a semicolon (;) delimited list of directories to exclude (including subdirectories):").split(";")
    sync_config["ssh_username"] = input("Enter your ssh user name:")
    sync_config["ssh_password"] = input("Enter your ssh password:")
    sync_config["threads"] = input("Enter the number of threads to use for transfers:")

    sync_dir = os.path.expanduser("~/.sync")
    if not os.path.exists(sync_dir):
        os.mkdir(sync_dir)
    save_config(sync_config,open(os.path.join(sync_dir,"sync_config"), "w"))
    return 0

def config( config_file, verbose ):
    """ load configuration for a backup from a config file """
    sync_config = {}
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
            sync_config[key] = value
    return sync_config
