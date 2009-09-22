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
	
	if (written == -1) {
		PyErr_SetString(PyExc_IOError, "Failed to write data to file");
		return NULL;
	} else {
		return Py_BuildValue("i", written);
	}
}


static PyObject *
hdfs_flush(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	PyObject *pyfile;
	hdfsFS fs;
	hdfsFile file;
	
	if (!PyArg_ParseTuple(args, "OO", &pyfs, &pyfile))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	file = (hdfsFile)PyLong_AsVoidPtr(pyfile);
	
	if (hdfsFlush(fs, file) != -1) {
		Py_RETURN_NONE;
	} else {
		PyErr_SetString(PyExc_IOError, "Failed to close file");
		return NULL;
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
		Py_RETURN_NONE;
	} else {
		PyErr_SetString(PyExc_IOError, "Failed to close file");
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
		Py_RETURN_NONE;
	} else {
		PyErr_SetString(PyExc_SystemError, "Failed to disconncect from hdfs");
		return NULL;
	}
}


/**
 * Copy file from hdfs to local.
 * @param fs The handle to hdfs.
 * @param rpath The path of hdfs file. 
 * @param lpath The path of local file. 
 * @return Returns None on success, NULL on error. 
 */
static PyObject *
hdfs_get(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	hdfsFS fs, lfs;
	const char *rpath, *lpath;
	
	if (!PyArg_ParseTuple(args, "Oss", &pyfs, &rpath, &lpath))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	lfs = hdfsConnect(NULL, 0);	/* connect to local fs */
	
	if (!lfs) {
		return NULL;
	}
	
	if (hdfsCopy(fs, rpath, lfs, lpath) != -1) {
		Py_RETURN_NONE;
	} else {
		PyErr_SetString(PyExc_IOError, "Failed to get file");
		return NULL;
	}
}


/**
 * Copy file from local to hdfs.
 * @param fs The handle to hdfs.
 * @param lpath The path of local file. 
 * @param rpath The path of hdfs file. 
 * @return Returns None on success, NULL on error. 
 */
static PyObject *
hdfs_put(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	hdfsFS fs, lfs;
	const char *lpath, *rpath;
	
	if (!PyArg_ParseTuple(args, "Oss", &pyfs, &lpath, &rpath))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	lfs = hdfsConnect(NULL, 0);	/* connect to local fs */
	
	if (!lfs) {
		return NULL;
	}
	
	if (hdfsCopy(lfs, lpath, fs, rpath) != -1) {
		Py_RETURN_NONE;
	} else {
		PyErr_SetString(PyExc_IOError, "Failed to put file");
		return NULL;
	}
}


/**
 * Checks if a given path exsits on the hdfs.
 * @param fs The configured filesystem handle.
 * @param path The path to look for
 * @return Returns True on success, False else.
 * int hdfsExists(hdfsFS fs, const char *path);
 */
static PyObject *
hdfs_exists(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	hdfsFS fs;
	const char *path;
	
	if (!PyArg_ParseTuple(args, "Os", &pyfs, &path))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	if (hdfsExists(fs, path) == 0) 
		Py_RETURN_TRUE;
	else
		Py_RETURN_FALSE;
}


static PyMethodDef HdfsMethods[] =
{
	{"connect", hdfs_connect, METH_VARARGS, "connect(host, port) -> fs \n\nConnect to a hdfs file system"},
	{"open", hdfs_open, METH_VARARGS, "open(fs, path, mode) -> hdfs-file \n\nOpen a hdfs file in given mode (\"r\" or \"w\")"},
	{"write", hdfs_write, METH_VARARGS, "write(fs, hdfsfile, str) -> byteswritten \n\nWrite data into an open file"},
	{"flush", hdfs_flush, METH_VARARGS, "flush(fs, hdfsfile) -> None \n\nFlush the data"},
	{"close", hdfs_close, METH_VARARGS, "close(fs, hdfsfile) -> None \n\nClose a hdfs file"},
	{"disconnect", hdfs_disconnect, METH_VARARGS, "disconnect(fs) -> None \n\nDisconnect from hdfs file system"},
//	{"test", hdfs_test, METH_VARARGS, "Run connect, open, read, write, close, disconnect in one time"},
	{"get", hdfs_get, METH_VARARGS, "get(fs, rpath, lpath) -> None \n\nCopy a file from hdfs to local"},
	{"put", hdfs_put, METH_VARARGS, "put(fs, lpath, rpath) -> None \n\nCopy a file from local to hdfs"},
	{"exists", hdfs_exists, METH_VARARGS, "exists(fs, path) -> True or False \n\nChecks if a given path exsits on the hdfs"},
	{NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC
initpyhdfs(void)
{
	(void) Py_InitModule("pyhdfs", HdfsMethods);
}

