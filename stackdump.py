# Copyright 2013-2014 James P Goodwin bkp@jlgoodwin.com
import sys
import traceback
import time

def stacktraces():
    f = open("stackdumps.out","a")
    print >>f, "="*80
    print >>f, "stacktraces start",time.ctime()
    for threadId, stack in sys._current_frames().items():
        print >>f, "-"*80
        print >>f, "# ThreadID: %s" % threadId
        print >>f, "-"*80
        for filename, lineno, name, line in traceback.extract_stack(stack):
            print >>f, 'File: "%s", line %d, in %s' % (filename, lineno, name)
            if line:
                print >>f,"  %s" % (line.strip())
    print >>f, "stacktraces end",time.ctime()
    print >>f, "="*80
    f.close()
