# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
import sys
import traceback
import time

def stacktraces():
    f = open("stackdumps.out","a")
    print("="*80, file=f)
    print("stacktraces start",time.ctime(), file=f)
    for threadId, stack in list(sys._current_frames().items()):
        print("-"*80, file=f)
        print("# ThreadID: %s" % threadId, file=f)
        print("-"*80, file=f)
        for filename, lineno, name, line in traceback.extract_stack(stack):
            print('File: "%s", line %d, in %s' % (filename, lineno, name), file=f)
            if line:
                print("  %s" % (line.strip()), file=f)
    print("stacktraces end",time.ctime(), file=f)
    print("="*80, file=f)
    f.close()
