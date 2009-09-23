#!/bin/sh

#install java
sudo apt-get install sun-java6-jdk

#check out the source
svn co http://libpyhdfs.googlecode.com/svn/trunk/ libpyhdfs

#prepare library files
cd libpyhdfs/lib
wget http://libpyhdfs.googlecode.com/files/commons-logging-1.0.4.jar
wget http://libpyhdfs.googlecode.com/files/hadoop-0.20.1-core.jar
wget http://libpyhdfs.googlecode.com/files/libhdfs.so.0
ln -s libhdfs.so.0 libhdfs.so

cd ..
  
#install
sudo python setup.py install --prefix=/usr/local

#set CLASSPATH
export LD_LIBRARY_PATH=/usr/local/lib/pyhdfs:/usr/lib/jvm/java-6-sun/jre/lib/i386/server
export CLASSPATH=/usr/local/lib/pyhdfs/hadoop-0.20.1-core.jar:/usr/local/lib/pyhdfs/commons-logging-1.0.4.jar:/etc/pyhdfs
