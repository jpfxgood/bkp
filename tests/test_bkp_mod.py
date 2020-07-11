from bkp_core import bkp_mod
from bkp_core import bkp_conf
from bkp_core import fs_mod
from bkp_core import util
from bkp_test_util import bkp_testdir
import platform
import os
import sys
import time
from io import StringIO
import math

def do_bkp_test( t_dir, base_path ):
    """ driver to test backup on different targets """
    bkp_config = {}
    bkp_config["bucket"] = base_path
    bkp_config["dirs"] = [t_dir["local_path"]]
    bkp_config["exclude_files"] = r"local_3\.txt"
    bkp_config["exclude_dirs"] = ["not_subdir_path"]
    bkp_config["log_email"] = t_dir["test_email"]
    bkp_config["error_email"] = t_dir["test_email"]
    bkp_config["ssh_username"] = t_dir["ssh_username"]
    bkp_config["ssh_password"] = t_dir["ssh_password"]
    bkp_config["threads"] = "5"

    end_time = time.time()

    bkp_job = bkp_mod.BackupJob(bkp_config)
    bkp_job.set_verbose(True)

    assert(not bkp_job.backup())

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
            assert(os.path.basename(lpath) in t_dir["local_files"])
            assert("local_3.txt" not in lpath)
            assert("not_subdir_path" not in lpath)
            date_count += 1
            backed_up_count += 1
        assert(date_count == 1)
    assert(backed_up_count == len(t_dir["local_files"])-1)

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
                assert(os.path.basename(local_path) in t_dir["local_files"])
                assert("local_3.txt" not in local_path)
                assert("not_subdir_path" not in local_path)
                backed_up_count += 1
    assert(backed_up_count == len(t_dir["local_files"])-1)
    assert(backup_count == 1)

    print("Overwrite the first local file!",file=open(os.path.join(t_dir["local_path"],t_dir["local_files"][0]),"w"))

    end_time = time.time()

    bkp_job_1 = bkp_mod.BackupJob(bkp_config)
    bkp_job_1.set_verbose(True)

    assert(not bkp_job_1.backup())

    next = util.get_contents( machine_path, "next", False, lambda: bkp_config )
    if next:
        second_bkp_time = float(next)
    else:
        second_bkp_time = 0.0

    assert(math.floor(second_bkp_time) >= math.floor(end_time))
    assert(math.floor(second_bkp_time) > math.floor(bkp_time))

    backed_up_count = 0
    backedup = bkp_mod.get_backedup_files(machine_path, bkp_config)
    for lpath,dates in list(backedup.items()):
        date_count = 0
        for d in dates:
            assert(os.path.basename(lpath) in t_dir["local_files"])
            assert("local_3.txt" not in lpath)
            assert("not_subdir_path" not in lpath)
            date_count += 1
            backed_up_count += 1
        assert(date_count == 1 or os.path.basename(lpath) == t_dir["local_files"][0])
    assert(backed_up_count == len(t_dir["local_files"]))

    backups = bkp_mod.get_backups( machine_path, bkp_config )

    backed_up_count = 0
    backup_count = 0
    for bkp in backups:
        assert(math.floor(bkp.time) >= math.floor(bkp_time) or math.floor(bkp.time) >= math.floor(second_bkp_time))
        bkp_log = util.get_contents(machine_path,bkp.timestamp+"/bkp/bkp."+bkp.timestamp+".log",False, lambda: bkp_config )
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
                assert(os.path.basename(local_path) in t_dir["local_files"])
                assert("local_3.txt" not in local_path)
                assert("not_subdir_path" not in local_path)
                backed_up_count += 1
    assert(backed_up_count == len(t_dir["local_files"]))
    assert(backup_count == 2)

    assert(not bkp_job_1.backup())
    backups = bkp_mod.get_backups( machine_path, bkp_config )
    assert(len(backups) == 3)

    old_stdout = sys.stdout
    file_list = StringIO()
    sys.stdout = file_list
    try:
        bkp_mod.list( bkp_config, True )
        sys.stdout = old_stdout
    finally:
        sys.stdout = old_stdout
    for l in file_list:
        parts = l.strip().split(" ",5)
        assert(os.path.basename(local_path) in t_dir["local_files"])

    bkp_mod.compact(bkp_config,False, True )
    backups = bkp_mod.get_backups( machine_path, bkp_config )
    assert(len(backups) == 2)


def test_bkp_mod_fs(bkp_testdir):
    """ test suite for the bkp_mod module covering file system functionality """
    do_bkp_test(bkp_testdir, bkp_testdir["file_basepath"])

def test_bkp_mod_ssh(bkp_testdir):
    """ test suite for the bkp_mod module covering ssh functionality """
    do_bkp_test(bkp_testdir, bkp_testdir["ssh_basepath"])

def test_bkp_mod_s3(bkp_testdir):
    """ test suite for the bkp_mod module covering s3 functionality """
    do_bkp_test(bkp_testdir, bkp_testdir["s3_basepath"])
