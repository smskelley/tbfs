#!/usr/bin/env python  
# -*- coding: utf-8 -*-  
import errno  
import fuse  
import stat  
import time  
import os,sys,glob
import hashlib

''' open_files is a dictionary for keeping track of which files are currently
open. The key indicates the *mounted* path, while the value is the file object.
'''
open_files={}
hash_dict = {}

fuse.fuse_python_api = (0, 2)  

class MyFS(fuse.Fuse):  

    def __init__(self, *args, **kw):  
        for x in sys.argv:
            print "***"+x
        print ":::::"+sys.argv[-2]
        fuse.Fuse.__init__(self, *args, **kw)  

    def getattr(self, path):  

        print "getattr-path: ",path
        return os.stat(sys.argv[-2]+path)

    def readdir(self,path,offset):
        print "*** READDIR: ",path

        yield fuse.Direntry('.')
        yield fuse.Direntry('..')
        print ":::::"+sys.argv[-2]
        for x in os.listdir(sys.argv[-2]+path):
            yield fuse.Direntry(os.path.basename(x))

        return

    def open(self,path,flags):

        print "********* OPEN: ",path

        access_flags = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
        access_flags = flags & access_flags

        if access_flags == os.O_RDONLY:
            fi=open(sys.argv[-2]+path,"r")
            open_files[path]=fi
            return 0

        else: 			#access_flags == os.O_WRONLY:

            fi=open(sys.argv[-2]+path,"w")
            open_files[path]=fi
            return 0


        return -errno.EACCESS

    def create(self, path, flags, mode):

        print "****CREATE: ",path
        fi=open(sys.argv[-2]+path,"w")
        open_files[path]=fi
        return 0

    def chmod(self, path, mode):
        print "*****CHMOD: ",path

        return 0

    def read(self,path,size,offset):

        print "****READ********: ",path,size,offset

        fi=open_files[path]
        fi.seek(offset)

        return fi.read(size)


    def write(self,path, buf, offset, fh=None):

        print "***WRITE: ",path,offset	

        fo=open_files[path]
        fo.seek(offset)
        fo.write(buf)
        return len(buf)

    def flush(self, path, fh=None):

        print "***FLUSH: ",path

        if path in open_files:
            fh=open_files[path]
            fh.flush()

        return 0

    def release(self, path, fh=None):
        ''' Close a particular file. Before closing, we will find its hash
        and if there's a file with that hash already, then we will discard
        this file. Otherwise, we will rename it to its hash value and store
        it in the hash_dict.
        '''
        print "***RELEASE: ",path

        if path in open_files:
            old_file_name = open_files[path].name
            previous_mode = open_files[path].mode
            open_files[path].close()
            del open_files[path]

            # if the file was only opened for reading, then we're done.
            if 'w' not in previous_mode and 'a' not in previous_mode:
                return 0

            with open(old_file_name, "r") as fh:
                hasher = hashlib.md5()
                for line in fh:
                    hasher.update(line)

                file_hash = hasher.hexdigest()

                if file_hash in hash_dict.values():
                    os.remove(old_file_name)
                else: 
                    os.rename(old_file_name, sys.argv[-2]+"/"+file_hash)

                hash_dict[path] = hash_dict
                print "***RELEASE: [{0}] -> [{1}]".format(path, file_hash)
        else:
            print "***RELEASE: no record of {0} being open.".format(path)

        return 0

    def unlink(self, path):

        print "***UNLINK: ",path

        if path in open_files:
            return -errno.ENOSYS

        os.unlink(sys.argv[-2]+path)
        return 0

    def rename(self, oldpath, newpath):

        os.rename(sys.argv[-2]+oldpath,sys.argv[-2]+newpath)
        return 0


if __name__ == '__main__':  
    fs = MyFS()  
    fs.parse(errex=1)  
    fs.main()  
