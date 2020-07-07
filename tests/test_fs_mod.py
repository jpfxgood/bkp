import os
from bkp_test_util import fs_testdir
from bkp_core import fs_mod
from io import StringIO
import time
import re

def test_fs_mod_ssh(fs_testdir):
    """ test suite for the fs_mod module covering sftp functionality """
    def get_config():
        """ return config settings """
        return fs_testdir

    def ssh_path(filename):
        """ return the ssh remote path for a given filename """
        return fs_testdir["ssh_basepath"]+'/'+filename

    def local_path(filename):
        """ return the local path for a given filename """
        return os.path.join(fs_testdir["local_path"],filename)

    for f in fs_testdir["remote_files"]:
        assert(fs_mod.fs_stat(ssh_path(f),get_config) == fs_testdir["remote_files_stats"][f])

    test_filename = local_path(fs_testdir["local_files"][1])
    test_remote_filename = ssh_path(fs_testdir["local_files"][1])

    file_stat = fs_mod.fs_stat(test_filename)
    fs_mod.fs_put(test_filename,test_remote_filename,get_config)
    fs_mod.fs_utime(test_remote_filename,(file_stat[0],file_stat[0]),get_config)
    assert(fs_mod.fs_stat(test_remote_filename,get_config) == file_stat)
    fs_mod.fs_del(test_remote_filename,get_config)
    assert(not fs_mod.fs_test(test_remote_filename,get_config))

    test_filename = local_path(fs_testdir["remote_files"][2])
    test_remote_filename = ssh_path(fs_testdir["remote_files"][2])
    file_stat = fs_mod.fs_stat(test_remote_filename,get_config)
    fs_mod.fs_get(test_remote_filename, test_filename, get_config)
    fs_mod.fs_utime(test_filename,(file_stat[0],file_stat[0]))
    assert(fs_mod.fs_stat(test_filename) == file_stat)

    remote_count = 0
    local_count = 0
    remote_list = StringIO(fs_mod.fs_ls(fs_testdir["ssh_basepath"]))
    for l in remote_list:
        mtime = time.mktime(time.strptime(l[:16],"%Y-%m-%d %H:%M"))
        parts = re.split("\s+",l,3)
        size = int(parts[2])
        file_name = os.path.basename(parts[3])
        if file_name in fs_testdir["remote_files"]:
            assert(fs_testdir["remote_files_stats"][file_name][1] == size)
            remote_count += 1
        elif file_name in fs_testdir["local_files"]:
            local_count += 1
    assert(remote_count == 5 and local_count == 0)

def test_fs_mod_s3(fs_testdir):
    """ test suite for the fs_mod module covering s3 functionality """
    pass


def test_fs_mod_file(fs_testdir):
    """ test suite for the fs_mod module covering local file system functionality """
    pass
