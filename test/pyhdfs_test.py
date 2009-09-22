#!/usr/bin/env python
import pyhdfs

host = "192.168.1.1"
port = 9000

def main():
    print "connecting"
    fs = pyhdfs.connect(host, port)
    
    print "opening"
    f = pyhdfs.open(fs, "/test/pyhdfs3", "w")
    
    print "writing"
    pyhdfs.write(fs, f, "hoho\0haha\nxixi")
    
    print "flushing"
    pyhdfs.flush(fs, f)
    
    print "closing"
    pyhdfs.close(fs, f)
    
    print "checking existence"
    if pyhdfs.exists(fs, "/test/hadoop-0.20.1.tar.gz"):
        print "getting"
        pyhdfs.get(fs, "/test/hadoop-0.20.1.tar.gz", "/tmp/hadoop.tgz")
    
    print "putting"
    pyhdfs.put(fs, "pyhdfs.so", "/test")
    
    print "disconnecting"
    pyhdfs.disconnect(fs)
    
if __name__ == "__main__":
    main()

