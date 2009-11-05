#!/usr/bin/env python
import time
import pyhdfs

host = "default"
port = 0

def main():
    print "connecting"
    fs = pyhdfs.connect(host, port)
    
    try:
        print "opening /test/foo for writing"
        f = pyhdfs.open(fs, "/test/foo", "w")
    
        print "writing"
        written = pyhdfs.write(fs, f, "hoho\0haha\nxixi")
        print "written %d bytes" % (written)
        
        print "flushing"
        pyhdfs.flush(fs, f)
    
        print "closing file"
        pyhdfs.close(fs, f)
    
        print "checking existence"
        if pyhdfs.exists(fs, "/test/foo"):
            print "getting"
            pyhdfs.get(fs, "/test/foo", "/tmp/foo.txt")
	    
	print "putting"
	pyhdfs.put(fs, "pyhdfs_test.py", "/test")
        
        print "opening /test/foo for reading"
        f = pyhdfs.open(fs, "/test/foo", "r")
        
        print "reading first 5 bytes"
        s = pyhdfs.read(fs, f, 5)
        print s, len(s)
        
        print "reading remaining"
        s = pyhdfs.read(fs, f)
        print s, len(s)
        
        print "telling"
        print pyhdfs.tell(fs, f)
        
        print "reading"
        s = pyhdfs.read(fs, f)
        print s, len(s)
        
        print "position reading from 5"
        s = pyhdfs.pread(fs, f, 5)
        print s, len(s)
        
        print "seeking"
        pyhdfs.seek(fs, f, 1)
        
        print "telling"
        print pyhdfs.tell(fs, f)
        
        print "closing file"
        pyhdfs.close(fs, f)

        print "updating file time"
        pyhdfs.utime(fs, "/test/foo", int(time.time()), int(time.time()))        
        
        print "stating file"
        print pyhdfs.stat(fs, "/test/foo")
        
	print "stating nosuchfile"
	print pyhdfs.stat(fs, "/test/nosuchfile")

	print "stating dir"
	print pyhdfs.stat(fs, "/test")

	print "mkdir dir /test/foo"
	print pyhdfs.mkdir(fs, "/test/foo")

	print "mkdir dir /test/dir/foo"
	print pyhdfs.mkdir(fs, "/test/dir/foo")
	print pyhdfs.stat(fs, "/test/dir/foo")

        print "listing directory"
        l = pyhdfs.listdir(fs, "/test")
        for i in l:
            print i
        
        
    finally:
        print "disconnecting"
        pyhdfs.disconnect(fs)
    
if __name__ == "__main__":
    main()

