Overview
========

This set of utilities is my own home grown attept to enable a local or offsite backup that runs automatically and backs up changed files over time. It has grown to include syncrhonization as well allowing a set of directories to be synced with a target server. 

Initially it was developed to work with the Amazon S3 storage service that is part of the Amazon Web Servvices platform, but then was expanded to allow for using sftp and local file systems.

When using the public cloud, the tools rely on the s3cmd tool and pgp encryption both of which need to be installed and set up. When using ssh they rely on the python package paramiko which is an all python implementation of ssh/sftp.

The unit of transfer in all cases is whole files and no attempt is made to handle collisions in the sync case. Local deletion on one of the clients is supported, i.e. files deleted on the client won't come back but they are not deleted on the server if they have been synced.

The backup creates a unique directory structure per backup and new copies of changed files are copied there. This creates a history of changes to files and the restore utility is capable of restoring previous versions of files as of a given date.

The sync utility keeps a set of directories in sync based on the modification dates of the files.

Typically to start using one of these utilies you create the config file and then run the utility in --dryrun --verbose mode which will output all of the actions it "would" perform to the standard error output.

It is recommended that you capture this output and review it to make sure the actions are sensible and tune the configuration with multiple dryruns until it is working well.

Then since the first backup or sync is usually the largest it is suggestd to run a non dryrun with the verbose output to monitor progress and let that run to completion.

After this initial run then bkp or sync can be added to a scheduler like cron and the incremental work will happend in the background.

Backup
======

    Usage: bkp [options]

    A backup script for backing up files to {s3,file, or sftp}, configuration in
    ~/.bkp/bkp_config

    Options:
      -h, --help            show this help message and exit
      -c, --configure       Prompt to create initial ~/.bkp/bkp_config
      -v, --verbose         Log all activity to console
      -d, --dryrun          Do everything except actually perform actions
      -f CONFIG_FILE, --file=CONFIG_FILE
                             Load config from this file default is
                             ~/.bkp/bkp_config
      -r RESTART_FILE, --restart=RESTART_FILE
                            Restart backup from this backup log file
      -K, --compact         Clean out empty backup directories
      -l, --list            list the files backed up for this machine and dates
                            available

    Template of config

    bucket = ssh://server-name:port/home/user/BkpVolume
    dirs =  /home/james
    exclude_files =  .*\.deb$|.*\.pyc$|.*\.so$|.*\.o$|.*\.la$|.*\.a$|.*\.[1-9]$|.*\.class$|.*\.jar$|.*\.war$
    exclude_dirs =  james/base;james/copier;james/data;james/fax;james/foscam;hplip\-3.12.6;james/installer;james/io;james/ip;james/old_boost;james/plugins;james/ppd;james/prnt;james/scan;james/subsonic;james/ui;james/ui4;james/zoneminder;james/doc;james/Downloads
    log_email =  backup@email-domain.com
    error_email =  user@email-domain.com
    threads =  5
    smtp_server =  smtp.email-domain.com
    smtp_username =  backup@email-domain.com
    smtp_password =  backup-password
    ssh_username = ssh_user
    ssh_password = ssh_pass
    end_config = True

bkp defaults to reading it's config from ~/.bkp/bkp_config this can be overidden using the -f/--file option and this file can be created using the -c/--configure option which will prompt for the items in the config.

In bkp_config the properties have the following meaning:

    bucket = The target root directory in remote storage where backups will be stored it should have the form {ssh://|file://|s3://}{server-name:port|path|bucket}{path}
    dirs = A semicolon delimited list of local directories to be backed up these should be fully qualified paths, all subdirectories of these paths will be processed
    exclude_files = This is a Python regular expression (see https://docs.python.org/2/library/re.html) which if it matches a file name will cause it to be excluded
    exclude_dirs = This is a list of Python regular expressions separated by semi-colons (;) which if they match any path will cause that path to be exlcuded
    log_email = Every successful backup will e-mail the backup log to this e-mail address
    error_email = Every failed backup will e-mail the exception to this e-mail address
    threads = Number of file transfer threads to use to process copies, recommended 5 for sftp and 10 for other targets
    smtp_server = Name of the smtp server to connect to for sending e-mail
    smtp_username = Name of the user to log in to smtp with
    smtp_password = Password of the user to log in to smtp with
    ssh_username = Name of the user to log in to ssh with
    ssh_password = Password of the user to log in to ssh with

bkp Options

Most of the options are self explanatory some more information is as below.

    -r RESTART_FILE, --restart=RESTART_FILE When a backup for a given time range fails it writes a log of the completed work and restart information to ~/.bkp/bkp.{date}.log.
    This file can be used to restart that backup using this command once the problem that caused the failure is resolved. The bkp process will also send e-mail if it finds a restart file or files on the machine.

    -K, --compact bkp will create empty backups sometimes, to speed up other processing running this once a week or so will clean up those empty directories.


Restore
=======

    Usage: rstr [options] restore_pattern { list of restore patterns }

    A restore script for restoring files backed up to {ssh,file, or s3} using bkp. A restore
    pattern is a python regular expression matching a path to restore ex:
    /home/james/My.*/.*\.jpg$

    Options:
      -h, --help            show this help message and exit
      -a ASOF, --asof=ASOF  date time to restore back to in YYYY.MM.DD.HH.MM.SS
                            format. Default is now.
      -v, --verbose         Log all activity to console
      -d, --dryrun          Do everything except actually perform actions
      -f CONFIG_FILE, --file=CONFIG_FILE
                            Load config from this file default is
                            ~/.bkp/bkp_config
      -m MACHINE, --machine=MACHINE
                            Machine name to restore for. Default is this machine.
      -p RESTORE_PATH, --path=RESTORE_PATH
                            Alternate existing target directory use as root for
                            restore. Default is the root directory of backed up
                            file.
      -e EXCLUDE, --exclude=EXCLUDE
                            Pattern on ENTIRE target path to exclude files from
                            restore. Use multiple args to add multiple filters.

    Restore refers to the same config file in ~/.bkp/bkp_config as bkp.

rstr Options

Most of the options are self expanatory, more information below.

    -m MACHINE, --machine=MACHINE If you are restoring onto a machine whose machine name is different this is the source machine's name.
    This is the name returned by python's platform.node() method and the linux hostname command.
    
    -p RESTORE_PATH, --path=RESTORE_PATH If you are restoring to a different directory than the original backup path, then this is the new root path.
    All target paths will be created as subdirectories of this path.

Sync
====

    Usage: sync [options]
    
    A synchronization script for synchronizing local and remote directories in {sftp, or file}, configuration in
    ~/.sync/sync_config
    
    Options:
      -h, --help            show this help message and exit
      -c, --configure       Prompt to create initial ~/.sync/sync_config
      -v, --verbose         Log all activity to console
      -d, --dryrun          Do everything except actually perform actions
      -f CONFIG_FILE, --file=CONFIG_FILE
                            Load config from this file default is
                            ~/.sync/sync_config

Sync compares the a list of local directories on the local machine with their counterparts on the remote machine and if they are newer it puts them to the remote machine, if they are older it gets them from the remote machine, and if there are files on the remote machine that don't exist it will get them.
These actions are subject to the exlude_files and exclude_dirs filters in the configuration. 
Any files deleted on the client machine (the one running sync) will not be synced in the future, but they will not be removed from the remote machine.

Sync config template:

    target = ssh://server-name:port
    dirs =  /home/user
    exclude_files =
    exclude_dirs = ^/home/user/(?!Documents[/]?).*
    threads =  5
    ssh_username =  ssh_user
    ssh_password =  ssh_pass
    end_config = True

Example crontab
===============

    # Edit this file to introduce tasks to be run by cron.
    #
    # Each task to run has to be defined through a single line
    # indicating with different fields when the task will be run
    # and what command to run for the task
    #
    # To define the time you can provide concrete values for
    # minute (m), hour (h), day of month (dom), month (mon),
    # and day of week (dow) or use '*' in these fields (for 'any').#
    # Notice that tasks will be started based on the cron's system
    # daemon's notion of time and timezones.
    #
    # Output of the crontab jobs (including errors) is sent through
    # email to the user the crontab file belongs to (unless redirected).
    #
    # For example, you can run a backup of all your user accounts
    # at 5 a.m every week with:
    # 0 5 * * 1 tar -zcf /var/backups/home.tgz /home/
    #
    # For more information see the manual pages of crontab(5) and cron(8)
    #
    # m h  dom mon dow   command
    0 6,10,14,18,22 * * * /bkp/bkp
    */30 * * * * /bkp/sync
