from bkp_core import bkp_mod
from bkp_core import bkp_conf
from bkp_core import fs_mod
from bkp_core import util
from bkp_test_util import bkp_testdir
import platform
import os
import time
from io import StringIO

def test_bkp_mod_fs(bkp_testdir):
    """ test suite for the bkp_mod module covering file system functionality """
    bkp_conf.bkp_config["bucket"] = bkp_testdir["file_basepath"]
    bkp_conf.bkp_config["dirs"] = [bkp_testdir["local_path"]]
    bkp_conf.bkp_config["exclude_files"] = "local_3\.txt"
    bkp_conf.bkp_config["exclude_dirs"] = ["not_subdir_path"]
    bkp_conf.bkp_config["log_email"] = bkp_testdir["test_email"]
    bkp_conf.bkp_config["error_email"] = bkp_testdir["test_email"]
    bkp_conf.bkp_config["ssh_username"] = bkp_testdir["ssh_username"]
    bkp_conf.bkp_config["ssh_password"] = bkp_testdir["ssh_password"]
    bkp_conf.bkp_config["threads"] = "5"

    end_time = time.time()

    assert(not bkp_mod.backup())

    machine_path = bkp_conf.get_config()["bucket"]+"/bkp/"+platform.node()
    next = util.get_contents( machine_path, "next", False)
    if next:
        bkp_time = float(next)
    else:
        bkp_time = 0.0

    assert(bkp_time >= end_time)

    backed_up_count = 0
    backedup = bkp_mod.get_backedup_files(machine_path)
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

    backups = bkp_mod.get_backups( machine_path )

    backed_up_count = 0
    backup_count = 0
    for bkp in backups:
        assert(bk.time >= end_time)
        bkp_log = get_contents(machine_path,bk.timestamp+"/bkp/bkp."+bk.timestamp+".log",False)

        past_config = False
        for l in bkp_log:
            backup_count += 1
            if not past_config:
                name,value = l.strip().split("=",1)
                name = name.strip()
                value = value.strip()
                if name == "end_config" and value == "True":
                    past_config = True
                    continue
                assert(bkp_conf.bkp_config[name] == value)
            else:
                local_path,remote_path,status,msg = l.split(";",3)
                assert(status != "error")
                assert(os.path.basename(local_path) in bkp_testdir["local_files"])
                assert("local_3.txt" not in local_path)
                assert("not_subdir_path" not in local_path)
                backed_up_count += 1
    assert(backed_up_count == len(bkp_testdir["local_files"])-1)
    assert(backup_count == 1)

    print("Overwrite the first local file!",open(os.path.join(bkp_testdir["local_path"],bkp_testdir["local_files"][0]),"w"))

    end_time = time.time()

    assert(not bkp_mod.backup())

    next = util.get_contents( machine_path, "next", False)
    if next:
        second_bkp_time = float(next)
    else:
        second_bkp_time = 0.0

    assert(second_bkp_time >= end_time)
    assert(second_bkp_time > bkp_time)

    backed_up_count = 0
    backedup = bkp_mod.get_backedup_files(machine_path)
    for lpath,dates in list(backedup.items()):
        date_count = 0
        for d in dates:
            assert(os.path.basename(lpath) in bkp_testdir["local_files"])
            assert("local_3.txt" not in lpath)
            assert("not_subdir_path" not in lpath)
            date_count += 1
            backed_up_count += 1
        assert(date_count == 1 or os.path.basename(lpath) == bkp_testdir["local_files"][0])
    assert(backed_up_count == len(bkp_testdir["local_files"]))

    backups = bkp_mod.get_backups( machine_path )

    backed_up_count = 0
    backup_count = 0
    for bkp in backups:
        assert(bk.time >= bkp_time or bk.time >= second_bkp_time)
        bkp_log = get_contents(machine_path,bk.timestamp+"/bkp/bkp."+bk.timestamp+".log",False)

        past_config = False
        for l in bkp_log:
            backup_count += 1
            if not past_config:
                name,value = l.strip().split("=",1)
                name = name.strip()
                value = value.strip()
                if name == "end_config" and value == "True":
                    past_config = True
                    continue
                assert(bkp_conf.bkp_config[name] == value)
            else:
                local_path,remote_path,status,msg = l.split(";",3)
                assert(status != "error")
                assert(os.path.basename(local_path) in bkp_testdir["local_files"])
                assert("local_3.txt" not in local_path)
                assert("not_subdir_path" not in local_path)
                backed_up_count += 1
    assert(backed_up_count == len(bkp_testdir["local_files"]))
    assert(backup_count == 2)
