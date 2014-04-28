#!/usr/bin/env python  
# -*- coding: utf-8 -*-  
import errno  
import fuse  
import stat  
import time  
import os,sys,glob
import hashlib
import pickle
import shutil
import random

''' open_files is a dictionary for keeping track of which files are currently
open. The key indicates the *mounted* path, while the value is the file object.
'''
open_files={}

fuse.fuse_python_api = (0, 2)  

class MyFS(fuse.Fuse):
    hash_pickle_file = ".hashdict.pickle"
    hash_dict = {}
    
    def __init__(self, *args, **kw):  
        for x in sys.argv:
            print "***"+x
        self.actual_path = os.path.abspath(sys.argv[-2])
        sys.argv[-1] = os.path.abspath(sys.argv[-1])
        print ":::::"+self.actual_path
        print ":::::"+sys.argv[-1]
        if self.load_data():
            print "Successfully loaded {0}".format(self.hash_pickle_file)

        fuse.Fuse.__init__(self, *args, **kw)

    def load_data(self):
        ''' Loads data from the hash_pickle_file located in the actual dir. '''
        if os.path.isfile(self.actual_file_path(self.hash_pickle_file)):
            with open(self.actual_file_path(self.hash_pickle_file), 'r') as fh:
                self.hash_dict = pickle.load(fh)
            return True
        return False
    
    def save_data(self):
        ''' Saves data to the hash_pickle_file located in the actual dir. '''
        with open(self.actual_file_path(self.hash_pickle_file), 'w') as fh:
            pickle.dump(self.hash_dict, fh)
            fh.flush()
            print "SAVE_DATA: saved to {0}".format(fh.name)

    def actual_file_path(self, actual_file):
        ''' Given an actual file (junk.txt), returns an absolute path to the
        actual file or path.
        '''
        #remove leading / if it exists, note that this isn't very portable.
        actual_file = actual_file.strip("/")
        return os.path.join(self.actual_path, actual_file)

    def getattr(self, path):  
        print "getattr-path: ",path
        print "\tIn hash_dict: ", self.hash_dict.get(path,"Not found")
        print "\tActual: ", self.actual_file_path(self.hash_dict.get(path,path))
        return os.stat(self.actual_file_path(self.hash_dict.get(path,path)))

    def readdir(self,path,offset):
        ''' Shows directory listing. Note that currently directories aren't
        implemented, so this simply lists all files.
        '''
        print "*** READDIR: ",path

        yield fuse.Direntry('.')
        yield fuse.Direntry('..')
        for key in self.hash_dict:
            yield fuse.Direntry(os.path.basename(key))

        return

    def open(self,path,flags):
        print "********* OPEN: ",path

        access_flags = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
        access_flags = flags & access_flags

        if access_flags == os.O_RDONLY:
            hash_path = self.hash_dict[path]
            fi=open(self.actual_file_path(hash_path),"r")
            open_files[path]=fi
            return 0

        else: 			#access_flags == os.O_WRONLY:
            randomnum = str(random.randint(0, sys.maxint))
            hash_path = self.hash_dict[path] + '_' + randomnum
            shutil.copyfile(self.actual_file_path(self.hash_dict[path]), 
                            self.actual_file_path(hash_path))
            fi=open(self.actual_file_path(hash_path),"w")
            open_files[path]=fi
            return 0


        return -errno.EACCESS

    def truncate(self, path, size):
        ''' Allows a file's size to be truncated. This is called when writing
        to an already existing file. (At least in linux)
        '''
        print "****TRUNCATE: {0}, size: {1}".format(path, size)
        with open(self.actual_file_path(self.hash_dict[path]), "w") as fh:
            os.ftruncate(fh.fileno(), size)
        return 0

    def create(self, path, flags, mode):
        print "****CREATE: ",path
        hash_path = "0_" + str(random.randint(0,sys.maxint))
        fi=open(self.actual_file_path(hash_path),"w")
        self.hash_dict[path] = hash_path
        open_files[path]=fi
        return 0

    def mkdir(self, path, mode):
        print "*****MKDIR: ",path
        if path not in self.hash_dict:
            self.hash_dict[path] = path
            return os.mkdir(self.actual_file_path(path), mode)

        return -errno.EEXIST    # if the path is already in path

    def chmod(self, path, mode):
        print "*****CHMOD: ",path
        if path in self.hash_dict:
            os.chmod(self.actual_file_path(self.hash_dict[path]), mode)
        else:
            return -errno.ENOENT

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

    def file_hash(self, path):
        with open(path, "r") as fh:
            hasher = hashlib.md5()
            for line in fh:
                hasher.update(line)
            return hasher.hexdigest()

    def release(self, path, fh=None):
        ''' Close a particular file. Before closing, we will find its hash
        and if there's a file with that hash already, then we will discard
        this file. Otherwise, we will rename it to its hash value and store
        it in the hash_dict.
        '''
        print "***RELEASE: ",path

        if path in open_files:
            old_file_name = self.hash_dict[path]
            open_file_name = os.path.basename(open_files[path].name)
            previous_mode = open_files[path].mode

            open_files[path].close()
            del open_files[path]

            if 'w' not in previous_mode and 'a' not in previous_mode:
                return 0
            
            file_hash = self.file_hash(self.actual_file_path(open_file_name))
            self.hash_dict[path] = ""

            # if the new file has the same hash as another file, discard it
            if file_hash in self.hash_dict.values():
                os.remove(self.actual_file_path(open_file_name))
            # the new file has a new, unique hash, save it as a new hash file
            else: 
                os.rename(self.actual_file_path(open_file_name),
                          self.actual_file_path(file_hash))

            # if the file has changed and the old file is no longer needed
            if (file_hash != old_file_name and 
                    old_file_name not in self.hash_dict.values() and
                    os.path.isfile(self.actual_file_path(old_file_name))):
                os.remove(self.actual_file_path(old_file_name))

            self.hash_dict[path] = file_hash
            print "***RELEASE: [{0}] -> [{1}]".format(path, file_hash)
            self.save_data()
        else:
            print "***RELEASE: no record of {0} being open.".format(path)

        return 0

    def unlink(self, path):
        print "***UNLINK: ",path

        if path in open_files:
            return -errno.ENOSYS 

        hash_file = self.hash_dict[path]
        del self.hash_dict[path]
		
        if hash_file not in self.hash_dict.values():
            os.unlink(self.actual_file_path(hash_file))
        
        self.save_data()
        return 0

    def rename(self, oldpath, newpath):
        hash_file = self.hash_dict[oldpath]
        self.hash_dict[newpath] = hash_file
        del self.hash_dict[oldpath]
        self.save_data()
        return 0


if __name__ == '__main__':  
    fs = MyFS()  
    fs.parse(errex=1)  
    fs.main()  
