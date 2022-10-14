#   Copyright (C) 2014 by Ronnie Sahlberg <ronniesahlberg@gmail.com>
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU Lesser General Public License as published by
#   the Free Software Foundation; either version 2.1 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU Lesser General Public License for more details.
#
#   You should have received a copy of the GNU Lesser General Public License
#   along with this program; if not, see <http://www.gnu.org/licenses/>.

import errno
import os
import sys
from .libnfs import *

def _stat_to_dict(stat):
        return {'dev': stat.nfs_dev,
                'ino': stat.nfs_ino,
                'mode': stat.nfs_mode,
                'nlink': stat.nfs_nlink,
                'uid': stat.nfs_uid,
                'gid': stat.nfs_gid,
                'rdev': stat.nfs_rdev,
                'size': stat.nfs_size,
                'blksize': stat.nfs_blksize,
                'blocks': stat.nfs_blocks,
                'atime': {'sec':  stat.nfs_atime,
                          'nsec': stat.nfs_atime_nsec},
                'ctime': {'sec':  stat.nfs_ctime,
                          'nsec': stat.nfs_ctime_nsec},
                'mtime': {'sec':  stat.nfs_mtime,
                          'nsec': stat.nfs_mtime_nsec}
                }


class NFSFH(object):
    def __init__(self, nfs, path, mode='r', codec=None):
        self._nfs = nfs
        self._name = path

        if codec:
            self._codec = codec
        elif sys.version_info[0] > 2:
            self._codec = 'utf-8'
        else:
            self._codec = None
        self._binary = True if 'b' in mode else False

        if path[:6] == "nfs://":
            _pos = path.rfind('/')
            _dir = path[:_pos]
            path = path[_pos:]
            self._private_context = NFS(_dir)
            self._nfs = self._private_context._nfs

        _plus = True if '+' in mode else False
        _mode = 0
        if 'r' in mode:
            _mode = os.O_RDWR if _plus else os.O_RDONLY
        if 'w' in mode:
            _mode = os.O_RDWR if _plus else os.O_WRONLY
            _mode |= os.O_CREAT|os.O_TRUNC
        if 'a' in mode:
            _mode = os.O_RDWR if _plus else os.O_WRONLY
            _mode |= os.O_CREAT|os.O_APPEND

        self._pnfsfh = new_NFSFileHandle()
        _status = nfs_open(self._nfs, path, _mode, self._pnfsfh)
        if _status == -errno.ENOENT and _mode & os.O_CREAT:
            _status = nfs_create(self._nfs, path, _mode, 0o664, self._pnfsfh)
        if _status < 0: 
            raise IOError(-ret, nfs_get_error(self._nfs))
        self._nfsfh = NFSFileHandle_value(self._pnfsfh)
        self._closed = False
        self._need_flush = False
        self._writing = True if _mode & (os.O_RDWR|os.O_WRONLY) else False

    def __del__(self):
        self.close()

    def close(self):
        if self._closed == False:
            if self._need_flush:
                self.flush()
            ret = nfs_close(self._nfs, self._nfsfh)
            #if ret < 0:
            #    raise IOError(-ret, nfs_get_error(self._nfs))
            if self._pnfsfh:
                delete_NFSFileHandle(self._pnfsfh)
            self._closed = True

    def write(self, data):
        if self._closed:
            raise ValueError('I/O operation on closed file')
        if not self._writing:
            raise IOError('Trying to write on file open for reading')

        if not isinstance(data, bytearray):
            if self._codec:
                data = bytearray(data.encode(self._codec))
            else:
                data = bytearray(data)
        ret = nfs_write(self._nfs, self._nfsfh, len(data), data)
        if ret < 0:
            raise IOError(-ret, nfs_get_error(self._nfs))
        self._need_flush = True
        return ret

    def read(self, size=-1):
        if self._closed:
            raise ValueError('I/O operation on closed file')
        if size < 0:
            _pos = self.tell()
            _st = nfs_stat_64()
            ret = nfs_fstat64(self._nfs, self._nfsfh, _st)
            if ret < 0:
                raise IOError(-ret, nfs_get_error(self._nfs))
            size = _st.nfs_size - _pos

        buf = bytearray(size)
        count = nfs_read(self._nfs, self._nfsfh, len(buf), buf)
        if count <0:
            raise IOError(-count, nfs_get_error(self._nfs))
        if self._binary:
            return buf[:count]

        if self._codec:
            return buf[:count].decode(self._codec)
        else:
            return str(buf[:count])

    def fstat(self):
        if self._closed:
            raise ValueError('I/O operation on closed file')
        _stat = nfs_stat_64()
        ret = nfs_fstat64(self._nfs, self._nfsfh, _stat)
        if ret < 0:
            raise IOError(-ret, nfs_get_error(self._nfs))
        return _stat_to_dict(_stat)

    def tell(self):
        if self._closed:
            raise ValueError('I/O operation on closed file')
        _pos = new_uint64_t_ptr()
        ret = nfs_lseek(self._nfs, self._nfsfh, 0, os.SEEK_CUR, _pos)
        if ret < 0:
            delete_uint64_t_ptr(_pos)
            raise IOError(-ret, nfs_get_error(self._nfs))
        ret = uint64_t_ptr_value(_pos)
        delete_uint64_t_ptr(_pos)
        return ret

    def seek(self, offset, whence=os.SEEK_CUR):
        if self._closed:
            raise ValueError('I/O operation on closed file')
        _pos = new_uint64_t_ptr()
        ret = nfs_lseek(self._nfs, self._nfsfh, offset, whence, _pos)
        if ret < 0:
            delete_uint64_t_ptr(_pos)
            raise IOError(-ret, nfs_get_error(self._nfs))
        delete_uint64_t_ptr(_pos)

    def truncate(self, offset=-1):
        if self._closed:
            raise ValueError('I/O operation on closed file')
        if not self._writing:
            raise IOError('Trying to truncate on file open for reading')
        if offset < 0:
            offset = self.tell()
        ret = nfs_ftruncate(self._nfs, self._nfsfh, offset)
        if ret < 0:
            raise IOError(-ret, nfs_get_error(self._nfs))

    def fileno(self):
        if self._closed:
            raise ValueError('I/O operation on closed file')
        _st = nfs_stat_64()
        ret = nfs_fstat64(self._nfs, self._nfsfh, _st)
        if ret < 0:
            raise IOError(-ret, nfs_get_error(self._nfs))
        return _st.nfs_ino

    def flush(self):
        if self._closed:
            raise ValueError('I/O operation on closed file')
        ret = nfs_fsync(self._nfs, self._nfsfh)
        if ret < 0:
            raise IOError(-ret, nfs_get_error(self._nfs))
        self._need_flush = False

    def isatty(self):
        return False

    @property
    def name(self):
        return self._name

    @property
    def closed(self):
        return self._closed

    @property
    def error(self):
        return nfs_get_error(self._nfs)


class NFS(object):
    def __init__(self, url):
        self._nfs = nfs_init_context()
        self._url = nfs_parse_url_dir(self._nfs, url)
        ret = nfs_mount(self._nfs, self._url.server, self._url.path)
        if ret < 0:
            raise IOError(-ret, nfs_get_error(self._nfs))
            # __del__ will be called after

    def __del__(self):
        if self._url:
            nfs_destroy_url(self._url)
        if self._nfs:
            nfs_destroy_context(self._nfs)

    def open(self, path, mode='r', codec=None):
        return NFSFH(self._nfs, path, mode=mode, codec=codec)

    def stat(self, path):
        _stat = nfs_stat_64()
        ret = nfs_stat64(self._nfs, path, _stat)
        if ret < 0: 
            raise IOError(-ret, nfs_get_error(self._nfs))
        return _stat_to_dict(_stat)

    def lstat(self, path):
        _stat = nfs_stat_64()
        ret = nfs_lstat64(self._nfs, path, _stat)
        if ret < 0: 
            raise IOError(-ret, nfs_get_error(self._nfs))
        return _stat_to_dict(_stat)

    def unlink(self, path):
        ret = nfs_unlink(self._nfs, path)
        if ret < 0: 
            raise IOError(-ret, nfs_get_error(self._nfs))

    def mkdir(self, path):
        ret = nfs_mkdir(self._nfs, path)
        if ret < 0: 
            raise IOError(-ret, nfs_get_error(self._nfs))

    def rmdir(self, path):
        ret = nfs_rmdir(self._nfs, path)
        if ret < 0: 
            raise IOError(-ret, nfs_get_error(self._nfs))

    def listdir(self, path):
        d = new_NFSDirHandle()
        ret = nfs_opendir(self._nfs, path, d)
        if ret < 0: 
            delete_NFSDirHandle(d)
            raise IOError(-ret, nfs_get_error(self._nfs))
        ret = []
        dv = NFSDirHandle_value(d)
        while True:
            de = nfs_readdir(self._nfs, dv)
            if de == None:
                break
            ret.append(de.name)
        delete_NFSDirHandle(d)
        return ret

    @property
    def error(self):
        return nfs_get_error(self._nfs)

def open(url, mode='r', codec=None):
    return NFSFH(None, url, mode=mode, codec=codec)

