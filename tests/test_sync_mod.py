import os
from bkp_test_util import sync_testdir
from bkp_core import sync_mod,util,fs_mod
from io import StringIO
import time
import re
import math

def test_sync_mod_fs(sync_testdir):
    """ test suite for the sync_mod testing for file system functionality """
    do_sync_mod_test(sync_testdir,sync_testdir["remote_fs_basepath"])

def test_sync_mod_ssh(sync_testdir):
    """ test suite for the sync_mod testing for sftp system functionality """
    do_sync_mod_test(sync_testdir,sync_testdir["ssh_basepath"])

def test_sync_mod_s3(sync_testdir):
    """ test suite for the sync_mod testing for file system functionality """
    do_sync_mod_test(sync_testdir,sync_testdir["s3_basepath"])

def do_sync_mod_test( t_dir, test_basepath ):
    """ driver to run sync_mod tests on various file systems based on the test_basepath parameter """
    sync_config = {}
    sync_config["target"] = test_basepath
    sync_config["dirs"] = [t_dir["local_path"]]
    sync_config["exclude_files"] = r"local_3\.txt"
    sync_config["exclude_dirs"] = ["not_subdir_path"]
    sync_config["ssh_username"] = t_dir["ssh_username"]
    sync_config["ssh_password"] = t_dir["ssh_password"]
    sync_config["threads"] = "5"

    sync_job = sync_mod.SyncJob( sync_config )
    sync_job.set_verbose(True)

    def get_paths( local_file ):
        if local_file.startswith("subdir_"):
            local_path = os.path.join(os.path.join(t_dir["local_path"],"subdir_path"),local_file)
            remote_path = os.path.join(os.path.join(sync_config["target"]+t_dir["local_path"],"subdir_path"),local_file)
        else:
            local_path = os.path.join(t_dir["local_path"],local_file)
            remote_path = os.path.join(sync_config["target"]+t_dir["local_path"],local_file)
        return (local_path,remote_path)

    deleted_on_client = []
    for cases in range(0,5):

        time.sleep(1) # guarantee at least 1 second between sync jobs
        assert(not sync_job.synchronize())

        for local_file in t_dir["local_files"]:
            local_path,remote_path = get_paths( local_file )
            if local_file == "local_3.txt":
                assert(fs_mod.fs_stat(local_path,lambda: sync_config) != (-1,-1))
                assert(fs_mod.fs_stat(remote_path,lambda: sync_config) == (-1,-1))
            else:
                local_mtime,local_size = fs_mod.fs_stat(local_path,lambda: sync_config)
                assert( (local_mtime,local_size) != (-1,-1))
                assert(fs_mod.fs_stat(remote_path,lambda: sync_config) == (local_mtime,local_size))

        for remote_file in t_dir["remote_files"]:
            local_path,remote_path = get_paths( remote_file )
            local_mtime,local_size = fs_mod.fs_stat(local_path,lambda: sync_config)
            assert( (local_mtime,local_size) != (-1,-1))
            assert(fs_mod.fs_stat(remote_path,lambda: sync_config) == (local_mtime,local_size))

        for l in StringIO(fs_mod.fs_ls(sync_config["target"],True,lambda: sync_config)):
            l = l.strip()
            parts = re.split(r"\s*",l,3)
            basename = os.path.basename(parts[-1])
            assert(basename.startswith(".sync") or basename in deleted_on_client or basename in t_dir["local_files"] or basename in t_dir["remote_files"])

        if cases == 0:
            local_path,remote_path = get_paths( "new_remote_file.txt" )
            util.put_contents(os.path.dirname(remote_path), os.path.basename(remote_path), "New remote content!", False, lambda: sync_config, False)
            t_dir["remote_files"].append( "new_remote_file.txt" )
        elif cases == 1:
            local_path,remote_path = get_paths( "local_4.txt" )
            print("New local_4.txt content", file=open(local_path,"w"))
        elif cases == 2:
            local_path,remote_path = get_paths( "local_4.txt" )
            assert(open(local_path,"r").read() == "New local_4.txt content\n")
            assert(util.get_contents(os.path.dirname(remote_path), os.path.basename(remote_path), False, lambda: sync_config) == "New local_4.txt content\n")
            fs_mod.fs_del(remote_path,False,lambda: sync_config)
            t_dir["local_files"].remove("local_4.txt")
            deleted_on_client.append("local_4.txt")
        elif cases == 3:
            local_path,remote_path = get_paths( "remote_3.txt" )
            fs_mod.fs_del(remote_path,False,lambda: sync_config)
