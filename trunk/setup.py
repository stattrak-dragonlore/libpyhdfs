from distutils.core import setup, Extension

module1 = Extension('pyhdfs',
                    sources = ['src/pyhdfs.c'],
                    include_dirs = ['/usr/lib/jvm/java-6-sun/include/'],
                    libraries = ['hdfs'],
                    library_dirs = ['lib'],
                    )

setup (name = 'PyHdfs',
       version = '0.1',
       author = 'Deng Zhiping'
       author_email = 'kofreestyler@gmail.com'
       ext_modules = [module1])
