#!/usr/bin/env python
import pyhdfs

host = "192.168.23.204"
port = 9000

def main():
    print "connecting"
    fs = pyhdfs.connect(host, port)
    
    try:
        print "opening"
        f = pyhdfs.open(fs, "/test/foo", "w")
    
        print "writing"
        pyhdfs.write(fs, f, "hoho\0haha\nxixi")
    
        print "flushing"
        pyhdfs.flush(fs, f)
    
        print "closing"
        pyhdfs.close(fs, f)
    
        print "checking existence"
        if pyhdfs.exists(fs, "/test/foo"):
            print "getting"
            pyhdfs.get(fs, "/test/foo", "/tmp/foo.txt")
	    
	print "putting"
	pyhdfs.put(fs, "pyhdfs_test.py", "/test")
    finally:
        print "disconnecting"
        pyhdfs.disconnect(fs)
    
if __name__ == "__main__":
    main()

