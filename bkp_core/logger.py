# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
import sys
import os
import re
import traceback
import queue
import threading
import platform
import time


logger_thread = None
logger_stop = False
logger_queue = queue.Queue()


def perform_log():
    """ read from the restore logging queue and print messages to stderr """
    while not stopped():
        line = get()
        if line:
            try:
                print(line, file=sys.stderr)
            except:
                print("Invalid Log Line!", file=sys.stderr)

def start_logger( action = perform_log ):
    """ start the restore logger thread """
    global logger_thread
    logger_thread = threading.Thread(target=action)
    logger_thread.start()

def stop_logger():
    """ stop the restore logger """
    global logger_stop
    logger_stop = True

def wait_for_logger():
    """ wait until the restore log queue is empty """
    global logger_thread
    global logger_stop
    global logger_queue

    if not logger_queue.empty():
        logger_queue.join()
    stop_logger()
    if logger_thread and logger_thread.is_alive():
        logger_thread.join()
    logger_thread = None
    logger_stop = False
    logger_queue = queue.Queue()


def log( msg ):
    """ log a message to the restore logger """
    logger_queue.put(msg)

def get():
    """ get a message off the queue """
    try:
        line = logger_queue.get(True,1)
        logger_queue.task_done()
    except queue.Empty:
        line = None
    return line


def stopped():
    """ test to see if we need to stop """
    return logger_stop
