from distutils.core import setup, Extension

pyhdfs = Extension('pyhdfs',
                   sources = ['src/pyhdfs.c'],
                   include_dirs = ['/usr/lib/jvm/java-6-sun/include/'],
                   libraries = ['hdfs'],
                   library_dirs = ['lib'],
                   runtime_library_dirs = ['/usr/local/lib/pyhdfs', '/usr/lib/jvm/java-6-sun/jre/lib/i386/server'],
                   )

setup(name = 'PyHdfs',
      version = '0.1',
      author = 'Deng Zhiping',
      author_email = 'kofreestyler@gmail.com',
      description = "Python wrapper for libhdfs",
      long_description = """libpyhdfs is a Python extension module which wraps
                             the C API in libhdfs to access Hadoop file system""",
      url = "http://code.google.com/p/libpyhdfs",
      license = "Apache License 2.0", 
      platforms = ["GNU/Linux"],
      ext_modules = [pyhdfs])
