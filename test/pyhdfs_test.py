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
    
    print "closing"
    pyhdfs.close(fs, f)
    
    print "disconnecting"
    pyhdfs.disconnect(fs)
    
if __name__ == "__main__":
    main()

