from __future__ import print_function, absolute_import, division

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
import time as _time
import os.path
import stat
import xmlrpclib,pickle, hashlib
from xmlrpclib import Binary
import math, socket, binascii
import numpy as np
import errno

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

BLOCKSIZE = 8 #byte
REDUNDANCY = 2

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Memory(LoggingMixIn, Operations):

    def __init__(self,*ports):
        #initialization for the system
        self.fd = 0
        now = time()
        self.N = 0 #initialized number of dataservers is zero.
        self.ports = ports #ports input from the terminal
        self.servSet = {} #
        self.Ports_dataServ = [] #stores the port number of each data server.
        for i in range(len(ports)-1):
            self.N += 1
        print(self.N) # the number of data servers
        # only been used in function: __init__, metaWrapper and dataPut,dataPop,dataGet
        # connecting to servers
        self.metaserver = xmlrpclib.ServerProxy('http://localhost:' + ports[0])
        conct_dataServs = {} # key is the port of one server, while the value is ports of other servers.
        #Connect to all of the dataServers.
        self.Ports_dataServ = ports[1:]
        for i in range(0,self.N):
            print(self.Ports_dataServ)
            cPort = self.Ports_dataServ[i]
            conct_dataServs[cPort] = xmlrpclib.ServerProxy('http://localhost:' + cPort)

        for i in range (0, self.N):
            self.servSet[self.Ports_dataServ[i]] = [] # each line is represented by a list: a list of servers
            for j in range(REDUNDANCY):
                self.servSet[self.Ports_dataServ[i]].append(conct_dataServs[self.Ports_dataServ[(i+j)%self.N]])

        metafiles = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2,Files = []) # no parent
        self.putmeta('/', metafiles)

    def getmeta(self, key):
        rv = self.metaserver.get(Binary(key))
        if rv != False:
            return pickle.loads(rv.data)
        else:
            return False

    def putmeta(self, key, meta):
        temp = Binary(pickle.dumps(meta, 2))
        return self.metaserver.put(Binary(key), temp)

    def popmeta(self, key):
        rv = self.metaserver.pop(Binary(key))
        if rv != False:
            return pickle.loads(rv.data)
        else:
            return False

    def getList(self):
        return pickle.loads(self.metaserver.pathList().data)

    def traverseparent(self, path):
        tar = path[path.rfind('/')+1:]
        p = path[:path.rfind('/')]
        if p == '':
            p = '/'
        return p, tar


    def dataGet(self,port,path,blks,version):
        arguments = {}
        blks = str(blks)#change the format before integrating the arguments.
        port = self.Ports_dataServ[port]
        redundant_servs = self.servSet[port] # the ports of redundant servers.
        port = str(port)
        arguments['blks'] = blks
        arguments['path'] = path
        arguments['port'] = port
        arguments['version'] = version
        argv = Binary(pickle.dumps(arguments,2))
        server_states = [] #accessible or not and data is correct or not.
        for i in range(REDUNDANCY*2):
            server_states.append(0)
        data={} #initialize the data
        timeCount = 0
        while(1 not in server_states):
            #try multiple times till it is timeout.
            if(timeCount >= 20):
                raise OSError(errno.ECONNREFUSED)
                break
            for i in range(REDUNDANCY):
                try:
                    message = redundant_servs[i].get(argv)
                    if message is False:
                        return ''
                    server_states[i] = 1
                    message = message.data
                    data[i] = message[:len(message)-8]
                    #last eight bytes are for crc
                    crc1 = message[len(message)-8:]
                    crc = self.checksum(data[i])
                    if(crc==crc1):
                        server_states[i+REDUNDANCY] = 1
                except socket.error:
                    pass
            #none of the servers are available, but not timeout yet.
            if (1 not in server_states):
                print("Trying to connect to the servers..........")
                _time.sleep(3)
                timeCount = timeCount + 3
            else:
                # find the correct return value
                for i in range(REDUNDANCY):
                    if server_states[i]==1 & server_states[i+REDUNDANCY]==1:
                        value = data[i]
                        break
                # possible that all servers crashed.
                if not (1 in (server_states)):
                    raise Exception("ALL Down!")
                for i in range(REDUNDANCY):
                    if server_states[i]==1:
                        if server_states[i+REDUNDANCY] == 0:
                            print("data corruption occurd in " + str(redundant_servs[i]))
                            arguments['data'] = value
                            argv = Binary(pickle.dumps(arguments,2))
                            redundant_servs[i].put(argv)
        return value

    def dataPop(self,port,path,blks,version):
        #Use get function to check and repair before next move.
        try:
            dataGet(port,path,blks,version)
        except OSError, e:
            if e.args[0] == errno.ECONNREFUSED:
                raise OSError(errno.ECONNREFUSED) # raise again, pass it to caller
        blks = str(blks)
        arguments = {}
        port = self.Ports_dataServ[port]
        redundant_servs = self.servSet[port] # the servers we need to read from
        port = str(port)
        arguments['blks'] = blks
        arguments['path'] = path
        arguments['port'] = port
        if version is not None:
            arguments['version']
        argv = Binary(pickle.dumps(arguments,2))
        # need a dictionary to store the data poped from all the servers.
        messages = {}
        for i in range(REDUNDANCY):
            messages[i] = redundant_servs[i].pop(argv)

        fcnt = 0 #count the number of servers failed.
        #if all of the servers failed, just return False.
        #otherwise, raise exception..
        for i in range(REDUNDANCY):
            if messages[i] == False:
                fcnt += 1
        #only handle the cases that the all pop failed of succeed.
        #check and repair will handle other cases.
        if fcnt == REDUNDANCY:
            return False
        if fcnt == 0:
            value = []
            for i in range(REDUNDANCY):
                message = messages[i].data
                value =message[:len(message)-8]
                crc1 = message[len(message)-8:]
                crc = self.checksum(value)
                if(crc==crc1):
                    return value
            raise Exception("Unexpected case!!")
        else:
            raise Exception("Unexpected case!!")

    def dataPut(self,port,path,blks,data,version):
        arguments = {}
        port = self.Ports_dataServ[port]
        redundant_servs = self.servSet[port] # the servers we need to read from
        length = len(data) # to be compared with returned acknowledgement
        blks = str(blks)
        port = str(port)
        arguments['blks'] = blks
        arguments['path'] = path
        arguments['port'] = port
        arguments['data'] = data
        arguments['version'] = version
        argv = Binary(pickle.dumps(arguments,2))
        server_states = []
        for i in range(REDUNDANCY):
            server_states.append(0)
        timeCount = 0
        while(0 in server_states):
            #Keep retrying within the timeout and do all-or-nothing after timeout.
            if(timeCount >= 20):
                # recover those servers succeed.
                for i in range(REDUNDANCY):
                    if server_states[i]==1:
                        redundant_servs[i].pop(argv)
                raise OSError(errno.ECONNREFUSED)
            for i in range(REDUNDANCY): #pick those redundant_servs that haven`t succeeded.
                if  server_states[i]==0:
                    try:
                        redundant_servs[i].put(argv)
                        server_states[i] = 1
                    except:
                        print("failed to connect to server" + str(i))
            if 0 in server_states:
                _time.sleep(2)
                timeCount = timeCount + 2
        return True

    def dataRename(self, port, old, blks, new):
        arguments = {}
        temp_arguments = {} #This dic store exact the same arguments, but exchange the old and new path for repair.
        port = self.Ports_dataServ[port]
        redundant_servs = self.servSet[port] # the servers we need to read from
        blks = str(blks)
        port = str(port)
        arguments['blks'] = blks
        arguments['old'] = old
        arguments['port'] = port
        arguments['new'] = new
        argv = Binary(pickle.dumps(arguments,2))
        temp_arguments['blks'] = blks
        temp_arguments['old'] = new
        temp_arguments['port'] = port
        temp_arguments['new'] = old
        argv1 = Binary(pickle.dumps(temp_arguments,2))
        server_states = []
        for i in range(REDUNDANCY):
            server_states.append(0) # accessible or not
        for i in range(REDUNDANCY):
            try:
                redundant_servs[i].keyRename(argv)
                server_states[i] = 1
            except:
                print("failed to connect to server" + i)
        #if any server failed to update, just withdraw all the updates.
        if 0 in server_states:
            for i in range(REDUNDANCY):
                if server_states[i] == 1:
                    redundant_servs[i].keyRename(argv1)
            raise OSError(errno.ECONNREFUSED)
        return True


    # CRC32
    def checksum(self,data):
        crc = binascii.crc32(data) & 0xffffffff
        return "{0:x}".format(crc).rjust(8,'0') # take it as a 8-byte string

    def chmod(self, path, mode):
        metafiles = self.getmeta(path)
        metafiles['st_mode'] &= 0o770000
        metafiles['st_mode'] |= mode
        self.putmeta(path, metafiles)


    def chown(self, path, uid, gid):
        metafiles = self.getdata(path)
        metafiles['st_uid'] = uid
        metafiles['st_gid'] = gid
        self.putdata(path, metafiles)

    def create(self, path, mode):
        metafiles = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        p, tar = self.traverseparent(path)
        metafiles['parent'] = p
        metafiles['startPortID'] = hash(path)%self.N
        metafiles['version'] = [] # a list of block version: length = numBlocks
        self.putmeta(path, metafiles)
        # modify parent meta
        pa_metafiles = self.getmeta(p)
        pa_metafiles['Files'].append(tar)
        self.putmeta(p, pa_metafiles)
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        metafiles = self.getmeta(path)
        if metafiles == False:
            raise FuseOSError(ENOENT)
        return metafiles

    def getxattr(self, path, name, position=0):
        metafiles = self.getmeta(path)
        attrs = metafiles.get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''

    def listxattr(self, path):
        metafiles = self.getmeta(path)
        attrs = metafiles.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        metafiles = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(),Files = [])
        p, tar = self.traverseparent(path)
        metafiles['parent'] = p
        self.putmeta(path, metafiles)
        # modify the parent metafiles

        pa_metafiles = self.getmeta(p)
        pa_metafiles['Files'].append(tar)
        pa_metafiles['st_nlink'] += 1
        self.putmeta(p, pa_metafiles)

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        content = self.distributedRead(path,size,offset,fh)
        return content

    #extention of the readfunction. Implements reading blocks one by one from distributed servers.
    def distributedRead(self, path, size, offset, fh):
        metafiles = self.getmeta(path)
        content = ''
        # There are two corner cases caused by offset and size.
        if offset >= metafiles['st_size']:
            return content
        if (offset+size) > metafiles['st_size']:
            size = metafiles['st_size']-offset

        startBlk = int(offset/BLOCKSIZE) # offset determine the first blockID to be read
        startOffset = offset%BLOCKSIZE

        # decides which server to read from first.
        start_port = metafiles['startPortID']
        start_port = (start_port + startBlk)%self.N

        lastBlk = int(math.ceil((offset+size)/BLOCKSIZE))
        endOffset = (offset+size)%BLOCKSIZE

        port = start_port # port, assistant varieble to traverse the dataservers
        state = [0] * (lastBlk-startBlk)
        #need stateArray to record the reading status of each block. initialized as 0. If success, it becomes 1
        # and it becomes 0 if fails.
        #reading each blk from different servers
        #since the reading of starting and ending block is affected by the offset, I operate them separately.
        #if it is between, I don`t worry about offset ,just read them all.
        for i in range(startBlk, lastBlk):
            if i != startBlk and i != lastBlk-1:
                content = content + self.dataGet(port,path,i,metafiles['version'][i])
            elif i == startBlk:
                content = self.dataGet(port,path,i,metafiles['version'][i])
                content = content[startOffset:]
            else:
                rv = self.dataGet(port,path,i,metafiles['version'][i])
                content = content + rv[:(size-len(content))]
            port = (port+1)%self.N

        return content

    def readdir(self, path, fh):
        metafiles = self.getmeta(path)
        return ['.', '..'] + metafiles['Files']

    def readlink(self, path):
        metafiles = self.getmeta(path)
        start_port = hash(path)%self.N
        data = self.dataGet(start_port,path,0,metafiles['version']) # index = 0
        return data

    def removexattr(self, path, name):
        metafiles = self.getmeta(path)
        attrs = metafiles.get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        #slice the new path and old path to get the parentdir and basename.
        p1, tar1 = self.traverseparent(old)
        p2, tar2 = self.traverseparent(new)
        #Taking the meta information of parent directories from metaserver.
        old_metafiles = self.getmeta(old)
        oldpa_metafiles = self.getmeta(p1)
        newpa_metafiles = self.getmeta(p2)
        new_metafiles = old_metafiles
        #First of all, remove the meta information of target we want to rename.
        #Then create metainformation of the target with a new name.
        print(newpa_metafiles)
        if tar1 in oldpa_metafiles['Files']:
            oldpa_metafiles['Files'].remove(tar1)
        if p1 == p2:
            newpa_metafiles['Files'].remove(tar1)
        if tar2 not in newpa_metafiles['Files']:
            newpa_metafiles['Files'].append(tar2)

        new_metafiles['parent'] = p2
        #I have to consider the situation that the rename is used in the move(),
        #which means the hard link of the directories have to change.
        #if it is a directory, you have to change pathinfo of all the files under that directory
        self.putmeta(new,new_metafiles)
        if p1 != p2:
            self.putmeta(p1,oldpa_metafiles) #This is after we remove tar1 from oldpa_metafiles.
        self.putmeta(p2,newpa_metafiles)

        # check whether the target file is directory or not.
        if stat.S_ISDIR(old_metafiles['st_mode']):
            #p1 != p2 is move, so the hard link will change
            if p1 != p2:
                oldpa_metafiles['st_nlink'] -=1
                newpa_metafiles['st_nlink'] +=1
            #when the target is a directory, we have to do recursive "rename" in the move().
            old_dir = old + '/'
            new_dir = new + '/'
            length = len(old_dir)
            allpath = self.getList()

            #check all the path in the filesyetem and get the path of the files in the target directory
            #and change the parent dir of those files.
            for path in allpath:
                if path.startswith(old_dir):
                    new_path = new_dir + path[length:]
                    #  change the head.
                    # update the regular file and do recursive on the directory.
                    temp_metafiles = self.getmeta(path)
                    temp_new_metafiles = temp_metafiles
                    if stat.S_ISREG(temp_metafiles['st_mode']):
                        self.rename(path,new_path)

                    # change the attribution "parent" for those files we renamed.
                    new_parent = new + temp_metafiles['parent'][len(old):]
                    temp_new_metafiles['parent'] = new_parent
                    #After everything is done, put the modified metafiles back to the new path.
                    self.popmeta(path)
                    if temp_metafiles!=False:
                        self.putmeta(new_path,temp_new_metafiles)

        else:
            # IF it is not a directory, just do all or nothing rename. Do the revert move on the part that succeeded.
            new_metafiles['startPortID'] = old_metafiles['startPortID']
            numblk = int(math.ceil(old_metafiles['st_size']/BLOCKSIZE))
            new_port = new_metafiles['startPortID']
            state = [0]*numblk
            #as long as one rename fail, do the repair immediately.
            for i in range(0,numblk):
                try:
                    self.dataRename(new_port, old, i, new)
                    state[i] = 1
                except OSError, e:
                    if e.args[0] == errno.ECONNREFUSED:
                        state[i] = -1
                        break
                new_port = (new_port+1)%self.N
            if -1 in state:
                new_port = new_metafiles['startPortID']
                for i in range(0,numblk):
                    if state[i] == 1:
                        self.dataRename(new_port, new, i, old)
                    new_port = (new_port+1)%self.N
                print('Error ')
                raise OSError(errno.ECONNREFUSED)
        self.popmeta(old)

    def rmdir(self, path):
        #This is also a recursive move for the directory.
        p, tar = self.traverseparent(path)
        metafiles = self.getmeta(path)
        pa_metafiles = self.getmeta(p)
        # update parent of path
        pa_metafiles['st_nlink'] -= 1
        pa_metafiles['Files'].remove(tar)

        path_dir = path + '/'
        length = len(path_dir)
        if len(metafiles['Files'])>0: #means this is a directory
            Files_list = metafiles['Files']
            for i in range(1,len(Files_list)):
                path_in_dir = path_dir + Files_list[i]
                path_in_dir_metafiles = getmeta(path_in_dir)
                if stat.S_ISDIR(path_in_dir_metafiles['st_mode']):
                    self.rmdir(path_in_dir)
                else:
                    # update both the data and meta servers.
                    self.popmeta(path_in_dir)
                    path_in_dir_metafiles = getmeta(path_in_dir)
                    start_port = path_in_dir_metafiles['startPortID']
                    fsize = path_in_dir_metafiles['st_size']
                    numblk = int(math.ceil(fsize/BLOCKSIZE))
                    port = start_port
                    for i in range(0, numblk):
                        self.dataPop(port,path_in_dir,i,None)
                        port = (port+1)%self.N

        self.popmeta(path)
        self.putmeta(p, pa_metafiles)

    def setxattr(self, path, name, value, options, position=0):
        metafiles = self.getmeta(path)
        attrs = metafiles.setdefault('attrs', {})
        attrs[name] = value
        self.putmeta(path, metafiles)

    def statfs(self, path):
        return dict(f_BLOCKSIZE=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        tarDic = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source), version=0)
        self.putmeta(target,tarDic)
        start_port = hash(target)%self.N
        self.dataPut(start_port, target, 0, source, 0)

    def truncate(self, path, length, fh=None):
        temp = ''
        allpath = self.getList()
        metafiles = self.getmeta(path)
        fsize = metafiles['st_size']
        csize = length - fsize
        #there are two situations.
        # 1. the length is larger than the file size, which means I have to add\x00 for complement
        # 2. the length is smaller than the file st_size
        #also, it is possible that the file doesn`t exist at all.
        if path in allpath:
            if length >fsize:
                temp.join('\x00'*csize)
                self.write(path,temp, fsize,fh)
        else:
            temp.join('\x00'*length)
            self.write(path,temp,0,fh)
        metafiles = self.getmeta(path)
        metafiles['st_size'] = length
        self.putmeta(path,metafiles)



    def unlink(self, path):
        self.popmeta(path)
        p, tar = self.traverseparent(path)
        #parentdir = os.path.abspath(os.path.join(path,os.pardir))
        pa_metafiles = self.getmeta(p)
        pa_metafiles['Files'].remove(tar)
        self.putmeta(p, pa_metafiles)

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        metafiles = self.getmeta(path)
        metafiles['st_atime'] = atime
        metafiles['st_mtime'] = mtime
        self.putmeta(path, metafiles)

    def write(self, path, data, offset, fh):
        #this part is just the extention of distributedwrite function
        metafiles = self.getmeta(path)
        _data = data
        # if the offset is larger than the file size we want to write on, complement it to offset with \x00
        if offset > metafiles['st_size']:
            _data = '\x00'*(offset-metafiles['st_size']) + data
            offset = metafiles['st_size']
            #since we already covered the difference between offset and file size
            #when we call the distributedwrite, the offset become file size.
        success_in_write = self.distributedWrite(path, _data,offset,fh)
        new_fsize = offset+len(data)
        if success_in_write:
            metafiles = self.getmeta(path)
            if new_fsize > metafiles['st_size']:
                metafiles['st_size'] = new_fsize
                self.putmeta(path,metafiles)
            return len(data)
        else:
            print("No connection!")
            raise OSError(errno.ECONNREFUSED)

    def distributedWrite(self, path,data,offset,fh):
        metafiles = self.getmeta(path)

        startBlk = int(offset/BLOCKSIZE) # offset determine the first blockID to be read
        startOffset = offset%BLOCKSIZE

        lastBlk = int(math.ceil((offset+len(data))/BLOCKSIZE))
        endOffset = (offset+len(data))%BLOCKSIZE

        numblk = lastBlk - startBlk
        start_port = metafiles['startPortID']
        start_port = (start_port + startBlk)%self.N
        port = start_port
        state = [0] * (numblk) #initialized as 0. 1 --- success  -1 -----fail
        index_of_block = 0 #this index is to record the index of block.
        version = metafiles['version']
        newVersion = version

        for i in range(startBlk, lastBlk):
            if i in range(len(version)):
                newVersion[i] = version[i] + 1
            else:
                newVersion.append(0)
        checkpoint = 0 # #counter tp recort how much of the data have been written
        # The following part is the same logic as distributedRead
        #1. the old version is modified before and need to update
        #2. this is the very first new version
        for i in range(startBlk,lastBlk):
            if i!=startBlk and i!=lastBlk-1:
                try:
                    self.dataPut(port,path,i,data[checkpoint:checkpoint+BLOCKSIZE], newVersion[i])
                    checkpoint = checkpoint+BLOCKSIZE
                    state[index_of_block] == 1
                except OSError, e:
                    if e.args[0] == errno.ECONNREFUSED:
                        state[index_of_block] = -1
                        print("NO connection!!!")
                        break
            elif i==startBlk:
                try:
                    temp = ''
                    if newVersion[i]!=0:
                        temp = self.dataGet(port,path,i,version[i])
                        temp = temp[:startOffset]
                    temp = temp + data[:BLOCKSIZE-startOffset]
                    checkpoint = BLOCKSIZE-startOffset
                    self.dataPut(port,path,i,temp, newVersion[i])
                    state[index_of_block] == 1
                except OSError, e:
                    if e.args[0] == errno.ECONNREFUSED:
                        state[index_of_block] = -1
                        print("NO connection")
                        break
            elif i==lastBlk-1: # for the last block
                try:
                    temp = ''
                    if newVersion[i]!=0:
                        temp = self.dataGet(port,path,i,version[i])
                        temp = temp[endOffset:]
                    temp = data[checkpoint:]+temp
                    checkpoint = checkpoint + len(data[checkpoint:])
                    self.dataPut(port,path,i,temp, newVersion[i])
                    state[index_of_block] == 1
                except OSError, e:
                    if e.args[0] == errno.ECONNREFUSED:
                        state[index_of_block] = -1
                        print("No connection..")
                        break
            #change the server after writing one block.
            port = (port+1)%self.N
            index_of_block += 1

        #when the distributedwrite failed in some of the servers, pop the block just wrote in.
        if -1 in state:
            port = metafiles['startPortID']
            for i in range(startBlk,lastBlk):
                if state[i]==1:
                    self.dataPop(port,path,i,newVersion[i])
                port = (port+1)%self.N
            return False
        else:
            # update the version list
            metafiles['version'] = newVersion
            self.putmeta(path,metafiles)
            return True



if __name__ == '__main__':
    if len(argv) < 4:
        print('usage: %s <mountpoint> <metaserver port> <dataserver1 port> .. <dataserverN port>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)
    ports = argv[2:]
    fuse = FUSE(Memory(*ports), argv[1], foreground=True, debug = True)
