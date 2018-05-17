import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary

class simpleHT:
  def __init__(self,port):
    self.data = {}
    print 'metaserver',port,'is online'
  def count(self):
    return len(self.data)

  def get(self, key):
    rv = False
    key = key.data
    if key in self.data:
      rv = Binary(pickle.dumps(self.data[key],2))
    return rv

  def put(self, key, value):
    self.data[key.data] = pickle.loads(value.data)
    return True

  def pop(self,key):
    key = key.data
    if key in self.data:
      key = self.data.pop(key)
      return Binary(pickle.dumps(key,2))
    else:
      return False


  def pathList(self):
    keys = self.data.keys()
    return Binary(pickle.dumps(keys,2))

def main():
  if len(sys.argv) != 2:
    print('usage: %s <metaserver port>' % sys.argv[0])
    exit(1)
  port = int(sys.argv[1])

  serve(port)

def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('localhost', port)) # localhost
  file_server.register_introspection_functions()
  sht = simpleHT(port)
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.pop)
  file_server.register_function(sht.pathList)

  try:
    file_server.serve_forever()
  except KeyboardInterrupt:
    print 'server crashed manually'


if __name__ == "__main__":
  main()
