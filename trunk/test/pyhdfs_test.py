#!/usr/bin/env python
import pyhdfs

host = "192.168.23.204"
port = 9000

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
        
        print "closing file"
        pyhdfs.close(fs, f)
        
    finally:
        print "disconnecting"
        pyhdfs.disconnect(fs)
    
if __name__ == "__main__":
    main()

