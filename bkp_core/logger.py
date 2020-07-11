# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
import sys
import os
import traceback
import queue
import threading

class Logger:
    def __init__( self ):
        self.logger_thread = None
        self.logger_stop = False
        self.logger_queue = queue.Queue()

    def perform_log( self ):
        """ read from the restore logging queue and print messages to stderr """
        while not self.stopped():
            line = self.get()
            if line:
                try:
                    print(line, file=sys.stderr)
                except:
                    print("Invalid Log Line!", file=sys.stderr)

    def start_logger( self, action = None ):
        """ start the restore logger thread """
        if not action:
            action = self.perform_log
        self.logger_thread = threading.Thread(target=action)
        self.logger_thread.start()

    def stop_logger( self ):
        """ stop the restore logger """
        self.logger_stop = True

    def wait_for_logger( self ):
        """ wait until the restore log queue is empty """

        if not self.logger_queue.empty():
            self.logger_queue.join()
        self.stop_logger()
        if self.logger_thread and self.logger_thread.is_alive():
            self.logger_thread.join()
        self.logger_thread = None
        self.logger_stop = False
        self.logger_queue = queue.Queue()

    def log( self, msg ):
        """ log a message to the restore logger """
        self.logger_queue.put(msg)

    def get( self ):
        """ get a message off the queue """
        try:
            line = self.logger_queue.get(True,1)
            self.logger_queue.task_done()
        except queue.Empty:
            line = None
        return line

    def stopped( self ):
        """ test to see if we need to stop """
        return self.logger_stop
