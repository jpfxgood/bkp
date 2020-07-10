from bkp_core import bkp_mod
from bkp_core import rstr_mod
from bkp_core import bkp_conf
from bkp_core import fs_mod
from bkp_core import util
from bkp_test_util import bkp_testdir
import platform
import os
import time
from io import StringIO
import math

def do_restore_test(t_dir,test_basepath):
    """ worker function to perform restore tests on various targets based on the setting of the base_path """
    bkp_config = {}
    bkp_config["bucket"] = test_basepath
    bkp_config["dirs"] = [t_dir["local_path"]]
    bkp_config["exclude_files"] = r"local_3\.txt"
    bkp_config["exclude_dirs"] = ["not_subdir_path"]
    bkp_config["log_email"] = t_dir["test_email"]
    bkp_config["error_email"] = t_dir["test_email"]
    bkp_config["ssh_username"] = t_dir["ssh_username"]
    bkp_config["ssh_password"] = t_dir["ssh_password"]
    bkp_config["threads"] = "5"

    bkp_job = bkp_mod.BackupJob(bkp_config)
    bkp_job.set_verbose(True)

    assert(not bkp_job.backup())
    machine_path = bkp_config["bucket"]+"/bkp/"+platform.node()

    backups = bkp_mod.get_backups( machine_path,bkp_config )
    assert(len(backups) == 1)

    restore_target = t_dir["testdir"].mkdir("restore_target")
    rstr_job = rstr_mod.RestoreJob( bkp_config )
    rstr_job.set_verbose(True)

    assert(not rstr_job.restore( restore_path=str(restore_target),restore_pats = [r".*/local_2\.txt"] ))
    assert(os.path.exists(os.path.join(str(restore_target)+t_dir["local_path"],"local_2.txt")))

    assert(not rstr_job.restore( restore_path=str(restore_target),restore_pats = [r".*"] ))

    def get_paths( local_file ):
        if local_file.startswith("subdir_"):
            original_file = os.path.join(os.path.join(t_dir["local_path"],"subdir_path"),local_file)
            restored_file = os.path.join(os.path.join(str(restore_target)+t_dir["local_path"],"subdir_path"),local_file)
        else:
            original_file = os.path.join(t_dir["local_path"],local_file)
            restored_file = os.path.join(str(restore_target)+t_dir["local_path"],local_file)
        return (original_file,restored_file)

    for local_file in t_dir["local_files"]:
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

    changed_file = os.path.join(t_dir["local_path"],t_dir["local_files"][0])
    print("Overwrite the first local file!",file=open(changed_file,"w"))

    assert(not bkp_job.backup())
    machine_path = bkp_config["bucket"]+"/bkp/"+platform.node()

    backups = bkp_mod.get_backups( machine_path,bkp_config )
    assert(len(backups) == 2)
    backups.sort(key=lambda backup: backup.time)

    restore_target = t_dir["testdir"].mkdir("restore_target_2")
    assert(not rstr_job.restore( restore_path=str(restore_target),restore_pats = [r".*"], asof=backups[0].timestamp ))

    for local_file in t_dir["local_files"]:
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

    assert(not rstr_job.restore( restore_path=str(restore_target),restore_pats = [r".*"], asof=backups[1].timestamp ))

    for local_file in t_dir["local_files"]:
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

def test_rstr_mod_fs(bkp_testdir):
    """ test suite for the rstr_mod module covering file system functionality """
    do_restore_test(bkp_testdir, bkp_testdir["file_basepath"])

def test_rstr_mod_ssh(bkp_testdir):
    """ test suite for the rstr_mod module covering ssh system functionality """
    do_restore_test(bkp_testdir, bkp_testdir["ssh_basepath"])

def test_rstr_mod_s3(bkp_testdir):
    """ test suite for the rstr_mod module covering s3 system functionality """
    do_restore_test(bkp_testdir, bkp_testdir["s3_basepath"])

