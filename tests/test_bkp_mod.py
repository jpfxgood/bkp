from bkp_core import bkp_mod
from bkp_core import bkp_conf
from bkp_core import fs_mod
from bkp_test_util import bkp_testdir
import platform
import os

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

    assert(not bkp_mod.backup())

    machine_path = bkp_conf.get_config()["bucket"]+"/bkp/"+platform.node()
    backedup = bkp_mod.get_backedup_files(machine_path)

    for lpath,dates in list(backedup.items()):
        date_count = 0
        for d in dates:
            assert(os.path.basename(lpath) in bkp_testdir["local_files"])
            assert("local_3.txt" not in lpath)
            assert("not_subdir_path" not in lpath)
            date_count += 1
        assert(date_count == 1)
