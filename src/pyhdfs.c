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
#include <sys/resource.h>
#include <stdlib.h>
#include <stdio.h>
#include "hdfs.h"

#define NO_JAVA_EXCEPTION_OUTPUT 1

FILE *
disable_stderr(void)
{
	if (NO_JAVA_EXCEPTION_OUTPUT) {
		return freopen("/dev/null", "w", stderr);
	}
}


FILE *
renable_stderr(void)
{
	if (NO_JAVA_EXCEPTION_OUTPUT) {
		return freopen("/dev/tty", "w", stderr);
	}
}


const char *
getfilepath(const char *url)
{
	/* hdfs://host:port/file/path */
	if (strncmp("hdfs://", url, 7) == 0)
		return strchr(url + 7, '/');
	else
		return url;
}


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
	tPort port;
	
	if (!PyArg_ParseTuple(args, "sH", &host, &port))
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
	const char *mode = "r";
	int bufsiz = 0;
	short rep = 0;
	tSize blksiz = 0;
	int flags = O_RDONLY;

	if (!PyArg_ParseTuple(args, "Os|sihi", &pyfs, &path, &mode, &bufsiz, &rep, &blksiz))
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
 * Read data from an open file
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param size read at most size bytes .
 * @return Returns the number of bytes read, NULL on error.
 */
static PyObject *
hdfs_read(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	PyObject *pyfile;
	hdfsFS fs;
	hdfsFile file;
	void *buf;
	int size;
	PyObject *res = NULL;

	
	if (!PyArg_ParseTuple(args, "OO|i", &pyfs, &pyfile, &size))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	file = (hdfsFile)PyLong_AsVoidPtr(pyfile);
	
	
	if (size > 2 * 1024 * 1024 || size <= 0)
		size = 2 * 1024 * 1024;
			
	buf = PyMem_Malloc(size);
	if (buf == NULL) 
		return PyErr_NoMemory();
	
	tSize bytesread = hdfsRead(fs, file, buf, size);
	if (bytesread == -1) {
		PyErr_SetString(PyExc_IOError, "Failed to read data from file");
	} else {
		res = Py_BuildValue("s#", buf, bytesread);
	}
	
	PyMem_Free(buf); 
	return res;
}


static PyObject *
hdfs_pread(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	PyObject *pyfile;
	hdfsFS fs;
	hdfsFile file;
	void *buf;
	tOffset offset;
	int size;
	PyObject *res = NULL;

	
	if (!PyArg_ParseTuple(args, "OOL|i", &pyfs, &pyfile, &offset, &size))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	file = (hdfsFile)PyLong_AsVoidPtr(pyfile);
	
	
	if (size > 2 * 1024 * 1024 || size <= 0)
		size = 2 * 1024 * 1024;
			
	buf = PyMem_Malloc(size);
	if (buf == NULL) 
		return PyErr_NoMemory();
	
	tSize bytesread = hdfsPread(fs, file, offset, buf, size);
	if (bytesread == -1) {
		PyErr_SetString(PyExc_IOError, "Failed to read data from file");
	} else {
		res = Py_BuildValue("s#", buf, bytesread);
	}
	
	PyMem_Free(buf); 
	return res;
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
	
	tSize written = hdfsWrite(fs, file, (void *)buf, siz);
	
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
hdfs_seek(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	PyObject *pyfile;
	hdfsFS fs;
	hdfsFile file;
	tOffset offset;
	
	if (!PyArg_ParseTuple(args, "OOL", &pyfs, &pyfile, &offset))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	file = (hdfsFile)PyLong_AsVoidPtr(pyfile);
	
	if (hdfsSeek(fs, file, offset) != -1) {
		Py_RETURN_TRUE;
	} else {
		Py_RETURN_FALSE;
	}
}


static PyObject *
hdfs_tell(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	PyObject *pyfile;
	hdfsFS fs;
	hdfsFile file;
	
	if (!PyArg_ParseTuple(args, "OO", &pyfs, &pyfile))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	file = (hdfsFile)PyLong_AsVoidPtr(pyfile);
	
	tOffset offset = hdfsTell(fs, file);
	
	if (offset != -1) {
		return Py_BuildValue("L", offset);
	} else {
		return Py_BuildValue("L", -1);
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
		Py_RETURN_TRUE;
	} else {
		Py_RETURN_FALSE;
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
		Py_RETURN_TRUE;
	} else {
		Py_RETURN_FALSE;
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
	if (hdfsExists(fs, path) != -1) 
		Py_RETURN_TRUE;
	else
		Py_RETURN_FALSE;
}


/**
 * Rename a file (directory)
 * @param fs The configured filesystem handle.
 * @param oldPath The path of the source file. 
 * @param newPath The path of the destination file. 
 * @return Returns 0 on success, -1 on error. 
 * int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath);
 */
static PyObject *
hdfs_rename(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	hdfsFS fs;
	const char *oldpath, *newpath;
	
	
	if (!PyArg_ParseTuple(args, "Oss", &pyfs, &oldpath, &newpath))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	if (hdfsRename(fs, oldpath, newpath) != -1)
		Py_RETURN_TRUE;
	else
		Py_RETURN_FALSE;
}


/**
 * Delete a file (directory)
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @return Returns True on success, False else.
 */
static PyObject *
hdfs_delete(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	hdfsFS fs;
	const char *path;
	
	if (!PyArg_ParseTuple(args, "Os", &pyfs, &path))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	if (hdfsDelete(fs, path) != -1) 
		Py_RETURN_TRUE;
	else
		Py_RETURN_FALSE;
}


static PyObject *
hdfs_stat(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	hdfsFS fs;
	const char *path;
	
	if (!PyArg_ParseTuple(args, "Os", &pyfs, &path))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	hdfsFileInfo *fileinfo = hdfsGetPathInfo(fs, path);
	
	if (fileinfo != NULL) {
		PyObject *res = Py_BuildValue("cLLL", fileinfo->mKind,
					      fileinfo->mSize,
					      (int64_t)fileinfo->mLastMod,
					      (int64_t)fileinfo->mLastAccess);
		hdfsFreeFileInfo(fileinfo, 1);
		return res;
	} else {
		Py_RETURN_NONE;
	}
}


static PyObject *
hdfs_mkdir(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	hdfsFS fs;
	const char *path;
	
	if (!PyArg_ParseTuple(args, "Os", &pyfs, &path))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);

	disable_stderr();
	if (hdfsCreateDirectory(fs, path) != -1) {
		renable_stderr();
		Py_RETURN_TRUE;
	} else {
		renable_stderr();
		Py_RETURN_FALSE;
	}
}


static PyObject *
hdfs_utime(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	hdfsFS fs;
	const char *path;
	int64_t mtime, atime;
	
	if (!PyArg_ParseTuple(args, "OsLL", &pyfs, &path, &mtime, &atime))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	
	if (hdfsUtime(fs, path, mtime, atime) != -1) {
		Py_RETURN_TRUE;
	} else {
		Py_RETURN_FALSE;
	}
}


/**
 * Get list of files/directories for a given path.
 * hdfsFreeFileInfo should be called to deallocate memory.
 * @param fs The configured filesystem handle.
 * @param path The path of the directory.
 * @return Returns a dynamically-allocated list of hdfsFileInfo
 * objects; NULL on error.
 */
static PyObject *
hdfs_listdir(PyObject *self, PyObject *args)
{
  	PyObject *pyfs;
	hdfsFS fs;
	const char *path;
	hdfsFileInfo *entries;
	int i;
	int num_entries;

	if (!PyArg_ParseTuple(args, "Os", &pyfs, &path))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	
	entries = hdfsListDirectory(fs, path, &num_entries);
	if (!entries && errno) {
		return PyErr_SetFromErrno(PyExc_IOError);
	} else {
		PyObject *py_entries = PyList_New(num_entries);
		for (i = 0; i < num_entries; i++) {
			PyObject *fields = Py_BuildValue(
				"{s:c,s:s,s:L,s:L,s:h,s:L,s:s,s:s,s:h,s:L}",
				"kind", entries[i].mKind,
				"name", entries[i].mName,
				"last_mod", (int64_t)entries[i].mLastMod,
				"size", entries[i].mSize,
				"replication", entries[i].mReplication,
				"block_size", entries[i].mBlockSize,
				"owner", entries[i].mOwner,
				"group", entries[i].mGroup,
				"permissions", entries[i].mPermissions,
				"last_access", (int64_t)entries[i].mLastAccess);
			PyList_SetItem(py_entries, i, fields);
		}
		hdfsFreeFileInfo(entries, num_entries);
		return py_entries;
	}
}


/**
 * Return a string representing the current working directory.
 * None on error
 */
static PyObject *
hdfs_getcwd(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	hdfsFS fs;
	void *buf;
	int size = 512;
	PyObject *res = NULL;
	
	if (!PyArg_ParseTuple(args, "O", &pyfs))
		return NULL;
	
	buf = PyMem_Malloc(size);
	if (buf == NULL) 
		return PyErr_NoMemory();
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	if (hdfsGetWorkingDirectory(fs, buf, size) != NULL) {
		res = Py_BuildValue("s", getfilepath(buf));
		PyMem_Free(buf); 
		return res;
	} else {
		PyMem_Free(buf); 		
		Py_RETURN_NONE;
	}
}


/**
 * Change the current working directory to the specified path.
 * The `path' may not exist.
 */
static PyObject *
hdfs_chdir(PyObject *self, PyObject *args)
{
	PyObject *pyfs;
	hdfsFS fs;
	const char *path;

	if (!PyArg_ParseTuple(args, "Os", &pyfs, &path))
		return NULL;
	
	fs = (hdfsFS)PyLong_AsVoidPtr(pyfs);
	
	disable_stderr();
	if (hdfsSetWorkingDirectory(fs, path) != -1) {
		renable_stderr();
		Py_RETURN_TRUE;
	} else {
		renable_stderr();		
		Py_RETURN_FALSE;
	}
}


static PyMethodDef HdfsMethods[] =
{
	{"connect", hdfs_connect, METH_VARARGS, "connect(host, port) -> fs \n\nConnect to a hdfs file system"},
	{"open", hdfs_open, METH_VARARGS, "open(fs, path[, mode[, bufsize[, replication[, blksiz]]]]) -> hdfs-file \n\nOpen a hdfs file in given mode (\"r\" or \"w\"), default is read-only"},
	{"write", hdfs_write, METH_VARARGS, "write(fs, hdfsfile, str) -> byteswritten \n\nWrite data into an open file"},
	{"flush", hdfs_flush, METH_VARARGS, "flush(fs, hdfsfile) -> None \n\nFlush the data"},
	{"read", hdfs_read, METH_VARARGS, "read(fs, hdfsfile[, size]) -> read at most min(2M, size) bytes, returned as a string \n\nIf the size argument is <=0 or omitted, read at most 2M bytes. When EOF is reached, empty string will be returned"},
	{"pread", hdfs_pread, METH_VARARGS, "pread(fs, hdfsfile, offset[, size]) -> similar to read, read data from given position"},
	{"seek", hdfs_seek, METH_VARARGS, "seek(fs, hdfsfile, offset) -> True or False \n\nSeek to given offset in open file in read-only mode"},
	{"tell", hdfs_tell, METH_VARARGS, "tell(fs, hdfsfile) -> int \n\nGet the current offset in the file, in bytes. -1 is returned on error"},
	{"close", hdfs_close, METH_VARARGS, "close(fs, hdfsfile) -> True or False \n\nClose a hdfs file"},
	{"disconnect", hdfs_disconnect, METH_VARARGS, "disconnect(fs) -> True or False \n\nDisconnect from hdfs file system"},
	{"get", hdfs_get, METH_VARARGS, "get(fs, rpath, lpath) -> None \n\nCopy a file from hdfs to local"},
	{"put", hdfs_put, METH_VARARGS, "put(fs, lpath, rpath) -> None \n\nCopy a file from local to hdfs"},
	{"delete", hdfs_delete, METH_VARARGS, "delete(fs, path) -> None \n\nDelete a file (directory)"},
	{"exists", hdfs_exists, METH_VARARGS, "exists(fs, path) -> True or False \n\nChecks if a given path exsits on the hdfs"},
	{"rename", hdfs_rename, METH_VARARGS, "rename(fs, oldpath, newpath) -> None \n\nRename a file (direcory)"},
	{"stat", hdfs_stat, METH_VARARGS, "stat(fs, path) -> fileinfo(type, size, lastmodify, lastaccess) \n\n Get information about a path"},
	{"mkdir", hdfs_mkdir, METH_VARARGS, "mkdir(fs, path) -> True or False \n\n Make the given path and all non-existent parents into directories"},
	{"utime", hdfs_utime, METH_VARARGS, "utime(fs, path, modtime, actime) -> True or False \n\nChange file last access and modification times"},
	{"listdir", hdfs_listdir, METH_VARARGS, "listdir(fs, path) -> [stats] \n\nGet list of files/directories of a given directory-path. Returns a list of dict object containing {kind, name, last_mod, size, replication, block_size, owner, group, permissions, last_access}"},
	{"getcwd", hdfs_getcwd, METH_VARARGS, "getcwd(fs) -> path \n\nReturn a string representing the current working directory."},
	{"chdir", hdfs_chdir, METH_VARARGS, "chdir(fs, path) -> True or False \n\nSet the working directory. The `path' can be a non-exist directory. All relative paths will be resolved relative to it."},
	{NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC
initpyhdfs(void)
{
	(void) Py_InitModule("pyhdfs", HdfsMethods);
	
	/* no core dump file */
	struct rlimit rlp;
	rlp.rlim_cur = 0;
	setrlimit(RLIMIT_CORE, &rlp);
}
