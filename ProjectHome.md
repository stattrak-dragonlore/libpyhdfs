libpyhdfs is a Python extension module which wraps the C API in libhdfs to access
HDFS (Hadoop file system).

Sample usage:
```
import pyhdfs

fs = pyhdfs.connect("192.168.1.1", 9000)
pyhdfs.get(fs, "/path/to/remote-src-file", "/path/to/local-file")

f = pyhdfs.open(fs, "/test/xxx", "w")
pyhdfs.write(fs, f, "fuck\0gfw\n")
pyhdfs.close(fs, f)

pyhdfs.disconnect(fs)
```


see [APIs](http://code.google.com/p/libpyhdfs/wiki/APIs) page for the supported APIs.

see [INSTALL](http://code.google.com/p/libpyhdfs/wiki/INSTALL) page for installation information.