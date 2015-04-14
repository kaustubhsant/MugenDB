import socket
import threading
import thread
from multiprocessing.pool import ThreadPool
from datetime import time
import json	
import logging
import time
import hashlib
import hash_ring
import traceback

masternum = 1

class MasterMonitorConnection:
    def __init__(self,portNumber):
        self.portNumber = portNumber
	self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)         
	self.host = socket.gethostname()               
	print self.sock.bind((self.host, self.portNumber)) 
        self.monitors = dict()
        with open("config/monitor.txt") as myfile:
            for line in myfile:
                name, endpoint = line.partition("=")[::2]
                self.monitors[name] = endpoint
        self.pool = ThreadPool(10) #TODO : configure this

    def find_slave_node(self,hxmd5):
	memcache_servers = []
	with open("config/slave.txt",'r') as myfile:
		for line in myfile:
			name, endpoint = line.partition("=")[::2]
			memcache_servers.append(endpoint)		
	ring = hash_ring.HashRing(memcache_servers)
	server = ring.get_node(hxmd5)
	return server

    def calculatemd5(self,key):
	m = hashlib.md5()
	m.update(key)
	return m.digest()

    def listen(self):
	print 'listening....' #TODO : log this
	#self.pool.apply_async(savekeymap,args=(self.keylocation,))
	while True:
	   	  request, addr = self.sock.recvfrom(1024)
		  print request
	    	  #self.pool.apply_async(ServeRequest, args=(request,self.monitor,keylocation))
		  rec_req = json.loads(request)
		  if rec_req['data'].keys():
			key = rec_req['data'].keys()[0]
		  else:
			key = rec_req['data']
		  hxmd5 = self.calculatemd5(key)		  
		  slave_node = self.find_slave_node(hxmd5)
		  print slave_node
		  rec_req['md5'] = hxmd5
		  rec_req['Master'] = masternum
		  print rec_req
		  #self.server.close()
		#connect to slave and send rec_req
		  '''
                  s = socket.socket()
		  host = socket.gethostname();
	       	  port = 10000
		  s.connect((host, port))
		  s.send(json.dumps(rec_req))
		  
                  if json.loads(s.recv(1024))==rec_req:
		     s.close()
		  else:
		     s.send(json.dumps(rec_req))
		  '''
	
if __name__ == "__main__":    
	s=MasterMonitorConnection(12345)
	s.listen()
