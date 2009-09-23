#!/bin/sh

#install java
sudo apt-get install sun-java6-jdk

#fetch hadoop
wget http://labs.xiaonei.com/apache-mirror/hadoop/core/hadoop-0.20.1/hadoop-0.20.1.tar.gz
tar xzf hadoop-0.20.1.tar.gz
 
#check out the source
svn co http://libpyhdfs.googlecode.com/svn/trunk/ libpyhdfs

#prepare library files
cd hadoop-0.20.1
cp c++/Linux-i386-32/lib/libhdfs.so.0 hadoop-0.20.1-core.jar lib/commons-logging-1.0.4.jar ../libpyhdfs/lib
cd ../libpyhdfs
ln -s libhdfs.so.0 lib/libhdfs.so
  
#install
sudo python setup.py install --prefix=/usr/local

#set CLASSPATH
export LD_LIBRARY_PATH=/usr/local/lib/pyhdfs:/usr/lib/jvm/java-6-sun/jre/lib/i386/server
export CLASSPATH=/usr/local/lib/pyhdfs/hadoop-0.20.1-core.jar:/usr/local/lib/pyhdfs/commons-logging-1.0.4.jar:/etc/pyhdfs

#run test script
cd test
python pyhdfs_test.py
   
