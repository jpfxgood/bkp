from bkp_core import bkp_mod
from bkp_core import bkp_conf
from bkp_core import sync_conf
from bkp_core import fs_mod
from bkp_core import util
from bkp_test_util import bkp_testdir, sync_testdir
import platform
import pexpect
import os
import re
import sys
import time
from io import StringIO
import math

def test_scripts_bkp ( bkp_testdir, request ):
    """ test suite to make sure that the scripts in the scripts folder actually work, this one is for bkp """
    python_path = os.path.dirname(os.path.dirname(request.fspath))
    exec_path = os.path.join(python_path,"scripts")
    os.environ["PYTHONPATH"] = python_path

    this_testdir = bkp_testdir["testdir"]
    config_path = str(this_testdir)+"/.bkp/bkp_config"

    bkp_config = {}
    bkp_config["bucket"] = "file://"+bkp_testdir["file_basepath"]
    bkp_config["dirs"] = [bkp_testdir["local_path"]]
    bkp_config["exclude_files"] = r"local_3\.txt"
    bkp_config["exclude_dirs"] = ["not_subdir_path"]
    bkp_config["log_email"] = bkp_testdir["test_email"]
    bkp_config["error_email"] = bkp_testdir["test_email"]
    bkp_config["ssh_username"] = bkp_testdir["ssh_username"]
    bkp_config["ssh_password"] = bkp_testdir["ssh_password"]
    bkp_config["threads"] = "5"

    bkp_conf.save_config(bkp_config, open(config_path,"w"))

    end_time = time.time()

    child = pexpect.spawnu(os.path.join(exec_path,'bkp')+' --verbose',env=os.environ)
    cmd_output = child.read()
    print(cmd_output)
    assert(child.isalive() == False)

    machine_path = bkp_config["bucket"]+"/bkp/"+platform.node()
    next = util.get_contents( machine_path, "next", False, lambda: bkp_config )
    if next:
        bkp_time = float(next)
    else:
        bkp_time = 0.0

    assert(math.floor(bkp_time) >= math.floor(end_time))

    backed_up_count = 0
    backedup = bkp_mod.get_backedup_files(machine_path, bkp_config)
    for lpath,dates in list(backedup.items()):
        date_count = 0
        for d in dates:
            assert(os.path.basename(lpath) in bkp_testdir["local_files"])
            assert("local_3.txt" not in lpath)
            assert("not_subdir_path" not in lpath)
            date_count += 1
            backed_up_count += 1
        assert(date_count == 1)
    assert(backed_up_count == len(bkp_testdir["local_files"])-1)

    backups = bkp_mod.get_backups( machine_path, bkp_config )

    backed_up_count = 0
    backup_count = 0
    for bkp in backups:
        assert(math.floor(bkp.time) >= math.floor(end_time))
        bkp_log = util.get_contents(machine_path,bkp.timestamp+"/bkp/bkp."+bkp.timestamp+".log",False, lambda: bkp_config)
        backup_count += 1

        past_config = False
        for l in StringIO(bkp_log):
            if not past_config:
                name,value = l.strip().split("=",1)
                name = name.strip()
                value = value.strip()
                if name == "end_config" and value == "True":
                    past_config = True
                    continue
                if name in ["dirs","exclude_dirs"]:
                    value = [f.strip() for f in value.split(";")]
                if name not in ["start_time","end_time"]:
                    assert(bkp_config[name] == value)
            else:
                local_path,remote_path,status,msg = l.split(";",3)
                assert(status != "error")
                assert(os.path.basename(local_path) in bkp_testdir["local_files"])
                assert("local_3.txt" not in local_path)
                assert("not_subdir_path" not in local_path)
                backed_up_count += 1
    assert(backed_up_count == len(bkp_testdir["local_files"])-1)
    assert(backup_count == 1)

    time.sleep(1)
    child = pexpect.spawnu(os.path.join(exec_path,'bkp')+' --verbose',env=os.environ)
    cmd_output = child.read()
    print(cmd_output)
    assert(child.isalive() == False)

    backups = bkp_mod.get_backups( machine_path, bkp_config )
    assert(len(backups) == 2)

    child = pexpect.spawnu(os.path.join(exec_path,'bkp')+' --list',env=os.environ)
    cmd_output = child.read()
    print(cmd_output)
    assert(child.isalive() == False)

    for l in StringIO(cmd_output):
        parts = l.strip().split(" ",5)
        assert(os.path.basename(parts[-1]) in bkp_testdir["local_files"])

    child = pexpect.spawnu(os.path.join(exec_path,'bkp')+' --compact',env=os.environ)
    cmd_output = child.read()
    print(cmd_output)
    assert(child.isalive() == False)

    backups = bkp_mod.get_backups( machine_path, bkp_config )
    assert(len(backups) == 1)

def test_scripts_rstr ( bkp_testdir, request ):
    """ test suite to make sure that the scripts in the scripts folder actually work, this one is for rstr """
    python_path = os.path.dirname(os.path.dirname(request.fspath))
    exec_path = os.path.join(python_path,"scripts")
    os.environ["PYTHONPATH"] = python_path

    this_testdir = bkp_testdir["testdir"]
    config_path = str(this_testdir)+"/.bkp/bkp_config"

    bkp_config = {}
    bkp_config["bucket"] = "file://"+bkp_testdir["file_basepath"]
    bkp_config["dirs"] = [bkp_testdir["local_path"]]
    bkp_config["exclude_files"] = r"local_3\.txt"
    bkp_config["exclude_dirs"] = ["not_subdir_path"]
    bkp_config["log_email"] = bkp_testdir["test_email"]
    bkp_config["error_email"] = bkp_testdir["test_email"]
    bkp_config["ssh_username"] = bkp_testdir["ssh_username"]
    bkp_config["ssh_password"] = bkp_testdir["ssh_password"]
    bkp_config["threads"] = "5"

    bkp_conf.save_config(bkp_config, open(config_path,"w"))

    child = pexpect.spawnu(os.path.join(exec_path,'bkp')+' --verbose',env=os.environ)
    cmd_output = child.read()
    print(cmd_output)
    assert(child.isalive() == False)

    machine_path = bkp_config["bucket"]+"/bkp/"+platform.node()
    backups = bkp_mod.get_backups( machine_path,bkp_config )
    assert(len(backups) == 1)

    restore_target = bkp_testdir["testdir"].mkdir("restore_target")
    child = pexpect.spawnu(os.path.join(exec_path,'rstr')+' --verbose --path="%s" ".*/local_2\.txt"'%restore_target,env=os.environ)
    cmd_output = child.read()
    print(cmd_output)
    assert(child.isalive() == False)

    assert(os.path.exists(os.path.join(str(restore_target)+bkp_testdir["local_path"],"local_2.txt")))

    child = pexpect.spawnu(os.path.join(exec_path,'rstr')+' --verbose --path="%s" ".*"'%restore_target,env=os.environ)
    cmd_output = child.read()
    print(cmd_output)
    assert(child.isalive() == False)

    def get_paths( local_file ):
        if local_file.startswith("subdir_"):
            original_file = os.path.join(os.path.join(bkp_testdir["local_path"],"subdir_path"),local_file)
            restored_file = os.path.join(os.path.join(str(restore_target)+bkp_testdir["local_path"],"subdir_path"),local_file)
        else:
            original_file = os.path.join(bkp_testdir["local_path"],local_file)
            restored_file = os.path.join(str(restore_target)+bkp_testdir["local_path"],local_file)
        return (original_file,restored_file)

    for local_file in bkp_testdir["local_files"]:
        original_file,restored_file = get_paths( local_file )

        if restored_file.endswith("local_3.txt"):
            assert(not os.path.exists(restored_file))
        else:
            assert(os.path.exists(restored_file))
            assert(open(original_file,"rb").read() == open(restored_file,"rb").read())
            orig_mtime,orig_size = fs_mod.fs_stat(original_file, lambda: bkp_config )
            restored_mtime,restored_size = fs_mod.fs_stat(restored_file, lambda: bkp_config )
            assert(orig_size == restored_size)
            assert(restored_mtime == backups[0].time)

    time.sleep(1) # guarantee at least 1 second between backup jobs

    changed_file = os.path.join(bkp_testdir["local_path"],bkp_testdir["local_files"][0])
    print("Overwrite the first local file!",file=open(changed_file,"w"))

    child = pexpect.spawnu(os.path.join(exec_path,'bkp')+' --verbose',env=os.environ)
    cmd_output = child.read()
    print(cmd_output)
    assert(child.isalive() == False)
    machine_path = bkp_config["bucket"]+"/bkp/"+platform.node()

    backups = bkp_mod.get_backups( machine_path,bkp_config )
    assert(len(backups) == 2)
    backups.sort(key=lambda backup: backup.time)

    restore_target = bkp_testdir["testdir"].mkdir("restore_target_2")
    child = pexpect.spawnu(os.path.join(exec_path,'rstr')+' --verbose --path="%s" --asof="%s" ".*"'%(restore_target,backups[0].timestamp),env=os.environ)
    cmd_output = child.read()
    print(cmd_output)
    assert(child.isalive() == False)

    for local_file in bkp_testdir["local_files"]:
        original_file,restored_file = get_paths( local_file )

        if restored_file.endswith("local_3.txt"):
            assert(not os.path.exists(restored_file))
        elif original_file == changed_file:
            assert(os.path.exists(restored_file))
            assert(open(original_file,"rb").read() != open(restored_file,"rb").read())
            orig_mtime,orig_size = fs_mod.fs_stat(original_file, lambda: bkp_config )
            restored_mtime,restored_size = fs_mod.fs_stat(restored_file, lambda: bkp_config )
            assert(orig_size != restored_size)
            assert(restored_mtime == backups[0].time)
        else:
            assert(os.path.exists(restored_file))
            assert(open(original_file,"rb").read() == open(restored_file,"rb").read())
            orig_mtime,orig_size = fs_mod.fs_stat(original_file, lambda: bkp_config )
            restored_mtime,restored_size = fs_mod.fs_stat(restored_file, lambda: bkp_config )
            assert(orig_size == restored_size)
            assert(restored_mtime == backups[0].time)

    child = pexpect.spawnu(os.path.join(exec_path,'rstr')+' --verbose --path="%s" --asof="%s" ".*"'%(restore_target,backups[1].timestamp),env=os.environ)
    cmd_output = child.read()
    print(cmd_output)
    assert(child.isalive() == False)

    for local_file in bkp_testdir["local_files"]:
        original_file,restored_file = get_paths( local_file )

        if restored_file.endswith("local_3.txt"):
            assert(not os.path.exists(restored_file))
        else:
            assert(os.path.exists(restored_file))
            assert(open(original_file,"rb").read() == open(restored_file,"rb").read())
            orig_mtime,orig_size = fs_mod.fs_stat(original_file, lambda: bkp_config )
            restored_mtime,restored_size = fs_mod.fs_stat(restored_file, lambda: bkp_config )
            assert(orig_size == restored_size)
            if original_file == changed_file:
                assert(restored_mtime == backups[1].time)
            else:
                assert(restored_mtime == backups[0].time)

def test_scripts_sync ( sync_testdir, request ):
    """ test suite to make sure that the scripts in the scripts folder actually work, this one is for sync """
    python_path = os.path.dirname(os.path.dirname(request.fspath))
    exec_path = os.path.join(python_path,"scripts")
    os.environ["PYTHONPATH"] = python_path

    this_testdir = sync_testdir["testdir"]
    if not os.path.exists(str(this_testdir)+"/.sync"):
        this_testdir.mkdir(".sync")
    config_path = str(this_testdir)+"/.sync/sync_config"

    sync_config = {}
    sync_config["target"] = sync_testdir["remote_fs_basepath"]
    sync_config["dirs"] = [sync_testdir["local_path"]]
    sync_config["exclude_files"] = r"local_3\.txt"
    sync_config["exclude_dirs"] = ["not_subdir_path"]
    sync_config["ssh_username"] = sync_testdir["ssh_username"]
    sync_config["ssh_password"] = sync_testdir["ssh_password"]
    sync_config["threads"] = "5"

    sync_conf.save_config(sync_config,open(config_path,"w"))

    child = pexpect.spawnu(os.path.join(exec_path,'sync')+' --verbose',env=os.environ)
    cmd_output = child.read()
    print(cmd_output)
    assert(child.isalive() == False)

    def get_paths( local_file ):
        if local_file.startswith("subdir_"):
            local_path = os.path.join(os.path.join(sync_testdir["local_path"],"subdir_path"),local_file)
            remote_path = os.path.join(os.path.join(sync_config["target"]+sync_testdir["local_path"],"subdir_path"),local_file)
        else:
            local_path = os.path.join(sync_testdir["local_path"],local_file)
            remote_path = os.path.join(sync_config["target"]+sync_testdir["local_path"],local_file)
        return (local_path,remote_path)

    for local_file in sync_testdir["local_files"]:
        local_path,remote_path = get_paths( local_file )
        if local_file == "local_3.txt":
            assert(fs_mod.fs_stat(local_path,lambda: sync_config) != (-1,-1))
            assert(fs_mod.fs_stat(remote_path,lambda: sync_config) == (-1,-1))
        else:
            local_mtime,local_size = fs_mod.fs_stat(local_path,lambda: sync_config)
            assert( (local_mtime,local_size) != (-1,-1))
            assert(fs_mod.fs_stat(remote_path,lambda: sync_config) == (local_mtime,local_size))

    for remote_file in sync_testdir["remote_files"]:
        local_path,remote_path = get_paths( remote_file )
        local_mtime,local_size = fs_mod.fs_stat(local_path,lambda: sync_config)
        assert( (local_mtime,local_size) != (-1,-1))
        assert(fs_mod.fs_stat(remote_path,lambda: sync_config) == (local_mtime,local_size))

    for l in StringIO(fs_mod.fs_ls(sync_config["target"],True,lambda: sync_config)):
        l = l.strip()
        parts = re.split(r"\s*",l,3)
        basename = os.path.basename(parts[-1])
        assert(basename.startswith(".sync") or basename in sync_testdir["local_files"] or basename in sync_testdir["remote_files"])
