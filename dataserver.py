import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
import shelve,binascii,socket
import numpy as np

REDUNDANCY = 2

class simpleHT:
  def __init__(self,*argv):
    # python dataserver.py 0 3333 4444 5555 6666
    #The first argument tells which one is the current server. e.g., 0 means port 3333 is the port
    #of current server.
    #In this project, there are 5 arguments, the first one is index and the following four are ports of the
    #data servers.
    current_server = argv[0]
    N = len(argv)-1
    Ports_dataServ = argv[1:]
    port = str(Ports_dataServ[current_server])
    print 'dataserver',port,'is online...'
    # Retrieve history data
    # self.data is a dict of dicts that has REDUNDANCY ports:data pairs
    # Actually, each server has its own data and the copy of last server's own data
    # {}, the filename is dataserver_port

    self.data = shelve.open('dataserver_'+port, writeback=True)

    # connect to the nextServer for recovery
    nextPort = str(Ports_dataServ[(current_server+1)%N])
    self.nextServer = xmlrpclib.ServerProxy('http://localhost:' + nextPort)

    self.ports = {}
    self.servers = {}
    for i in range(REDUNDANCY):
      self.ports[i] = str(Ports_dataServ[(current_server-i)%N]) # ports[0] is the port of current dataserver
      if i!= 0:
        self.servers[self.ports[i]] = xmlrpclib.ServerProxy('http://localhost:' + self.ports[i])
    #connect to the all the other dataservers
    #check whether the data dic is empty. If it is empty, initialize them. get the replica from nextserver.
    #Otherwise, get the replica from other servers.
    if self.data!={}:
      print("Loaded previous data from server backup file")
    else:
      for i in range(REDUNDANCY):
        self.data[self.ports[i]] = {}

      for i in self.ports:
        p = self.ports[i]
        if p == port:
          try:
            replica = self.nextServer.getReplica(Binary(port))
            self.data[p] = pickle.loads(replica.data)
            print 'Successfully loaded previous data from dataserver:', nextPort
          except socket.error:
            print 'Recovering previous data is passed'
            pass
        else:
          try:
            replica = self.servers[p].getReplica(Binary(p))
            self.data[p] = pickle.loads(replica.data)
            print "Successfully loaded previous copy from dataserver:", p
          except socket.error:
            print "Recovering previous copy is passed"
            pass


  def get(self,argv):
    rv = False
    datagram = pickle.loads(argv.data)
    port = datagram['port']
    path = datagram['path']
    ID = datagram['blks']
    version = datagram['version']
    #check whether there is the specified block in the specified path on the specified server. Also,
    #need to make sure it has the version.
    if port in self.data and path in self.data[port] and \
        ID in self.data[port][path] and \
        version in self.data[port][path][ID]:
      rv = Binary(self.data[port][path][ID][version])
    return rv

  # Put a new version of block
  # return the length of received data
  def put(self,argv):
    datagram = pickle.loads(argv.data)
    port = datagram['port']
    path = datagram['path']
    ID = datagram['blks']
    version = datagram['version']
    value = datagram['data']
    length = str(len(value))
    if path not in self.data[port]: # initialize a dict for a new file
      self.data[port][path] = {}
    if ID not in self.data[port][path]:
      self.data[port][path][ID] = {} # initialize a dict for a new block
    self.data[port][path][ID][version] = value + self.checksum(value)
    return Binary(length)


  def pop(self,argv):
    rv = False
    datagram = pickle.loads(argv.data)
    port = datagram['port']
    path = datagram['path']
    ID = datagram['blks']
    # version is the blocks of data from different time line. Each time it is modified, the version increases by one.
    if port in self.data and path in self.data[port] and ID in self.data[port][path]:
      if 'version' in datagram  and datagram['version'] in self.data[port][path][ID]:
        data = self.data[port][path][ID].pop(datagram['version'])
        rv = Binary(data)
      else:
        data = self.data[port][path].pop(ID)
        rv = Binary(data)
    return rv

  def keyRename(self,argv):
    rv = False
    datagram = pickle.loads(argv.data)
    port = datagram['port']
    old = datagram['old']
    new = datagram['new']
    ID = datagram['blks']
    if port in self.data and old in self.data[port] and ID in self.data[port][old]:
      if new not in self.data[port]:
        self.data[port][new] = {}
      self.data[port][new][ID] = self.data[port][old].pop(ID)
      if self.data[port][old] == {}:
        self.data[port].pop(old)

  def getReplica(self,port):
    port = str(port.data)
    replica = self.data[port]
    replica = pickle.dumps(replica,2)
    return Binary(replica)

  def corrupt(self,path):

    rv = False
    p = []
    for i in self.ports:
      if path in self.data[self.ports[i]]:
        p.append(i)
    if len(p)>0:
      s = np.random.randint(0,len(p))
      s = p[s]

      # randomly pick up a block
      IDlist = self.data[self.ports[s]][path].keys()
      ID = np.random.randint(0,len(IDlist))
      ID = IDlist[ID]
      # randomly pick up a byte
      version = len(self.data[self.ports[s]][path][ID])-1
      mes = self.data[self.ports[s]][path][ID][version]
      byte = np.random.randint(0,len(mes)-1)
      i = 1
      while(mes[byte]==str(i)):
        i = i+1
      mes = mes[:byte] + str(i) + mes[byte+1:]
      self.data[self.ports[s]][path][ID][version] = mes
      rv = True
      print('Information:')
      print 'Corrupted file:', path
      print 'Corrupted blks: ', ID
    else:
      print("Path is unfound.")
    return rv




#CRC32 for checksum
  def checksum(self,data):
    crc = binascii.crc32(data) & 0xffffffff
    return "{0:x}".format(crc).rjust(8,'0') # take it as a 8-byte string


def main():
  if len(sys.argv) < 4:
    print('usage: %s <indexed server number> <ports for all dataservers sperated by spaces>' % sys.argv[0])
    exit(1)

  sys.argv[1:] = map(int, sys.argv[1:]) # convert the arguments into integers

  serve(*sys.argv)

def serve(*argv):


  index = argv[1] # The first argument is the index of this dataserver.
  port = argv[index+2] # The first port start from the index 2.
  argv = argv[1:] # from index to all the data servers.

  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('localhost', port),allow_none=True)
  file_server.register_introspection_functions()
  sht = simpleHT(*argv)
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.pop)
  file_server.register_function(sht.getReplica)
  file_server.register_function(sht.corrupt)
  file_server.register_function(sht.keyRename)

  try:
    file_server.serve_forever()
  except KeyboardInterrupt:
    print 'server crashed manually..'

if __name__ == "__main__":
  main()
