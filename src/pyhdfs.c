/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <Python.h>
#include <stdio.h>
#include "hdfs.h"


/**
 * Connect to the hdfs file system.
 * @param host A string containing either a host name, or an ip address
 * of the namenode of a hdfs cluster. 
 * @param port The port on which the server is listening.
 * @return Returns a handle to the filesystem or NULL on error.
 */
static PyObject *
hdfs_connect(PyObject *self, PyObject *args)
{
	const char *host;
	int port;
	
	if (!PyArg_ParseTuple(args, "si", &host, &port))
		return NULL;
	
	hdfsFS fs = hdfsConnect(host, port);
	if(!fs) {
		PyErr_Format(PyExc_SystemError, "Failed to conncect to %s:%d", host, port);
		return NULL;
	} 	
	
	return PyLong_FromVoidPtr((void *)fs);
}


/**
 * Open a hdfs file in given mode.
 * @param fs The configured filesystem handle.
 * @param path The full path to the file.
 * @param flags - an | of bits/fcntl.h file flags - supported flags are O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT), 
 * O_WRONLY|O_APPEND. Other flags are generally ignored other than (O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno equal ENOTSUP.
 * @param bufferSize Size of buffer for read/write - pass 0 if you want
 * to use the default configured values. (optional)
 * @param replication Block replication - pass 0 if you want to use
 * the default configured values. (optional)
 * @param blocksize Size of block - pass 0 if you want to use the
 * default configured values. (optional)
 * @return Returns the handle to the open file or NULL on error.
 */
static PyObject *
hdfs_open(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	hdfsFS fs;
	const char *path;
	const char *mode;
	int bufsiz = 0;
	int rep = 0;
	int blksiz = 0;
	int flags = O_RDONLY;

	if (!PyArg_ParseTuple(args, "Oss|iii", &pyfs, &path, &mode, &bufsiz, &rep, &blksiz))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);

	if (!strcmp(mode, "r")) {
		flags = O_RDONLY;
	} else if (!strcmp(mode, "w")) {
		flags = O_WRONLY;
	} else if (!strcmp(mode, "a")) {
		/* append is disabled by default 
		   flags = O_WRONLY | O_APPEND; */
		PyErr_SetString(PyExc_ValueError, "File append is not supported yet");
		return NULL;
	} else {
		/* bad open mode */
		PyErr_SetString(PyExc_ValueError, "Unknown file open mode");
		return NULL;
	}
	
	hdfsFile file = hdfsOpenFile(fs, path, flags, bufsiz, rep, blksiz);
	if(!file) {
		PyErr_SetString(PyExc_IOError, "Failed to open file");
		return NULL;
        }
	return PyLong_FromVoidPtr(file);
}


static PyObject *
hdfs_read(PyObject *self, PyObject *args);


/**
 * Write data into an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param buffer The data.
 * @param length The no. of bytes to write. 
 * @return Returns the number of bytes written, NULL on error.
 */
static PyObject *
hdfs_write(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	PyObject *pyfile;
	hdfsFS fs;
	hdfsFile file;
	const char *buf;
	int siz;
	
	if (!PyArg_ParseTuple(args, "OOs#", &pyfs, &pyfile, &buf, &siz))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	file = (hdfsFile)PyLong_AsVoidPtr(pyfile);
	
	size_t written = hdfsWrite(fs, file, (void *)buf, siz);
	/* need flush? */
	/* hdfsFlush(fs, file); */
	
	if (written == -1) {
		return NULL;
	} else {
		return Py_BuildValue("i", written);
	}
}


static PyObject *
hdfs_close(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	PyObject *pyfile;
	hdfsFS fs;
	hdfsFile file;
	
	if (!PyArg_ParseTuple(args, "OO", &pyfs, &pyfile))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	file = (hdfsFile)PyLong_AsVoidPtr(pyfile);
	
	if (hdfsCloseFile(fs, file) != -1) {
		Py_INCREF(Py_None);
		return Py_None;
	} else {
		return NULL;
	}
}


/**
 * Disconnect from hdfs file system.
 * @param fs The configured filesystem handle.
 * @return Returns 0 on success, -1 on error.  
 */
static PyObject *
hdfs_disconnect(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	hdfsFS fs;
	
	if (!PyArg_ParseTuple(args, "O", &pyfs))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	if (hdfsDisconnect(fs) != -1) {
		Py_INCREF(Py_None);
		return Py_None;
	} else {
		return NULL;
	}
}


static PyObject *
hdfs_test(PyObject *self, PyObject *args)
{
	const char *host;
	int port;
	
	if (!PyArg_ParseTuple(args, "si", &host, &port))
		return NULL;
	
	hdfsFS fs = hdfsConnect(host, port);
	if(!fs) {
		char msg[128];
		snprintf(msg, sizeof(msg), "Failed to conncect to %s:%d", host, port);
		PyErr_SetString(PyExc_SystemError, msg);
		return NULL;
	} 	
	hdfsFile file = hdfsOpenFile(fs, "/test/shit", O_WRONLY, 0, 2, 0);
	if(!file) {
		PyErr_SetString(PyExc_IOError, "Failed to open file");
		return NULL;
        }
	
	char* buffer = "holyyyyyyy shit!!!!!!!!!!!";
        hdfsWrite(fs, file, (void*)buffer, strlen(buffer)+1);
	hdfsCloseFile(fs, file);
	hdfsDisconnect(fs);
	Py_INCREF(Py_None);
	return Py_None;
}


static PyMethodDef HdfsMethods[] =
{
	{"connect", hdfs_connect, METH_VARARGS, "Connect to a hdfs file system"},
	{"open", hdfs_open, METH_VARARGS, "Open a hdfs file in given mode"},
//	{"read", hdfs_read, METH_VARARGS, "Read data from an open file"},
	{"write", hdfs_write, METH_VARARGS, "Write data into an open file"},
	{"close", hdfs_close, METH_VARARGS, "Close a hdfs file"},
	{"disconnect", hdfs_disconnect, METH_VARARGS, "Disconnect from hdfs file system"},
	{"test", hdfs_test, METH_VARARGS, "Run connect, open, read, write, close, disconnect in one time"},
	{NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC
initpyhdfs(void)
{
	(void) Py_InitModule("pyhdfs", HdfsMethods);
}

