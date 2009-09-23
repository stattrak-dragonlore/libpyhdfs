from distutils.core import setup, Extension

pyhdfs = Extension('pyhdfs',
                   sources = ['src/pyhdfs.c'],
                   include_dirs = ['/usr/lib/jvm/java-6-sun/include/'],
                   libraries = ['hdfs'],
                   library_dirs = ['lib'],
                   )

files=[('lib/pyhdfs', ['lib/hadoop-0.20.1-core.jar', 'lib/commons-logging-1.0.4.jar',
                       'lib/libhdfs.so.0']),
       ('/etc/pyhdfs', ['conf/hdfs-site.xml'])]

setup (name = 'PyHdfs',
       version = '0.1',
       author = 'Deng Zhiping',
       author_email = 'kofreestyler@gmail.com',
       description = "Python wrapper for libhdfs",
       long_description = """libpyhdfs is a Python extension module which wraps
                             the C API in libhdfs to access Hadoop file system""",
       url = "http://code.google.com/p/libpyhdfs",
       license = "Apache License 2.0", 
       platforms = ["GNU/Linux"],
       data_files = files,
       ext_modules = [pyhdfs])
