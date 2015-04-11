import socket
import threading
import thread
from multiprocessing.pool import ThreadPool
from datetime import time
import json	
from api_func import MugenDBAPI
import logging
import time
import traceback
from hash_ring import MemcacheRing

#data = dict{}
masternum =1

class MasterMonitorConnection:
    def __init__(self,portNumber):
        self.portNumber = portNumber
	self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)         
	self.host = socket.gethostname()               
	self.sock.bind((self.host, self.portNumber)) 
        self.monitors = dict()
        with open("config/monitor.txt") as myfile:
            for line in myfile:
                name, endpoint = line.partition("=")[::2]
                self.monitors[name] = endpoint
        myfile.close()
        self.pool = ThreadPool(10) #TODO : configure this

    def listen(self):
	print 'listening....' #TODO : log this
	#self.pool.apply_async(savekeymap,args=(self.keylocation,))
	while True:
		  client,address = self.server.accept()
	   	  	#request, addr = self.sock.recvfrom(1024)
	    	  #self.pool.apply_async(ServeRequest, args=(request,self.monitor,keylocation))
		  rec_req = json.loads(client.recv(1024))
		  if rec_req['data'].keys():
			key = rec_req['data'].keys()[0]
		  else:
			key = rec_req['data']
		  hxmd5 = calculatemd5(key)
		  rec_req['md5'] = hxmd5
		  rec_req['Master'] = masternum
		  self.server.close()
		#connect to slave and send rec_req
		  
                  s = socket.socket()
		  host = socket.gethostname();
	       	  port = 10000
		  s.connect((host, port))
		  s.send(json.dumps(rec_req))
		  
                  if json.loads(s.recv(1024))==rec_req
		     s.close()
		  else
		     s.send(json.dumps(rec_req))


def calculatemd5(key)
    	memcache_servers = ['localhost:10000']
	ring = HashRing(memcache_servers)
	server = ring.get_node('key')
	
