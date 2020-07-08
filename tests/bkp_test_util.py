from bkp_core import fs_mod
import pytest
import os
import shutil

@pytest.fixture(scope="function")
def fs_testdir(request,testdir):
    sftp_basepath = os.environ.get("SSH_BASEPATH",None)
    sftp_username = os.environ.get("SSH_USERNAME",None)
    sftp_password = os.environ.get("SSH_PASSWORD",None)
    s3_bucket = os.environ.get("S3_BUCKET",None)
    s3_config = os.environ.get("S3_CONFIG",None)

    assert sftp_basepath and sftp_username and sftp_password,"SSH_* environment not set"
    assert s3_bucket and s3_config, "S3 environment not set"

    shutil.copyfile(s3_config,os.path.join(str(testdir.tmpdir),".s3cfg"))

    local_files = []
    remote_files = []
    local_file_names = []
    remote_file_names = []
    remote_files_stats = {}
    for i in range(0,5):
        args = { "local_%d"%(i):"\n".join(["local_%d test line %d"%(i,j) for j in range(0,200)])}
        local_files.append(testdir.makefile(".txt",**args))
        args = { "remote_%d"%(i):"\n".join(["local_%d test line %d"%(i,j) for j in range(0,200)])}
        remote_files.append(testdir.makefile(".txt",**args))
    for f in remote_files:
        file_stat = fs_mod.fs_stat(str(f))
        remote_files_stats[f.basename] = file_stat
        fs_mod.fs_put( str(f), sftp_basepath+str(f),lambda : { "ssh_username" : sftp_username, "ssh_password" : sftp_password})
        fs_mod.fs_utime( sftp_basepath+str(f), (file_stat[0],file_stat[0]), lambda : { "ssh_username" : sftp_username, "ssh_password" : sftp_password})
        fs_mod.fs_put( str(f), s3_bucket+str(f),lambda : { "ssh_username" : sftp_username, "ssh_password" : sftp_password})
        remote_file_names.append(f.basename)
        f.remove()
    for f in local_files:
        local_file_names.append(f.basename)

    def cleanup_sftp_testdir():
        fs_mod.fs_del( sftp_basepath+str(testdir.tmpdir.parts()[1]),True, lambda : { "ssh_username" : sftp_username, "ssh_password" : sftp_password })
        fs_mod.fs_del( s3_bucket+str(testdir.tmpdir.parts()[1]),True, lambda : { "ssh_username" : sftp_username, "ssh_password" : sftp_password })
        os.remove(os.path.join(str(testdir.tmpdir),".s3cfg"))

    request.addfinalizer(cleanup_sftp_testdir)

    return {"ssh_username" : sftp_username,
            "ssh_password" : sftp_password,
            "ssh_basepath": sftp_basepath+str(testdir.tmpdir),
            "s3_basedpath": s3_bucket+str(testdir.tmpdir),
            "local_path": str(testdir.tmpdir),
            "local_files" : local_file_names,
            "remote_files" : remote_file_names,
            "remote_files_stats" : remote_files_stats,
            "testdir" : testdir }
