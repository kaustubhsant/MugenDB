import socket
import threading
import thread
from multiprocessing.pool import ThreadPool
from datetime import time
import json	
import logging
import time
import hashlib
from hash_ring import HashRing
import traceback

masternum = 1
#intialize logging
log_filename = 'logs/'+'slave-log.txt'
logging.basicConfig(filename = log_filename,filemode = 'a',level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('MasterMonitorConnection.py')

class MasterMonitorConnection:
    def __init__(self,portNumber):
        self.portNumber = portNumber
	self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)         
	self.host = socket.gethostname()  
	print self.host             
	self.sock.bind((self.host, self.portNumber)) 
        self.monitors = dict()
        with open("config/monitor.txt") as myfile:
            for line in myfile:
                name, endpoint = line.partition("=")[::2]
		print endpoint
                self.monitors[name] = endpoint
        self.pool = ThreadPool(10) #TODO : configure this
	self.memcache_servers = []
	self.slave_nodes = dict()
	with open("config/slave.txt",'r') as myfile:
		for line in myfile:
			name, endpoint = line.partition("=")[::2]
			self.memcache_servers.append(endpoint)
			self.slave_nodes[name]=endpoint
	self.ring = HashRing(self.memcache_servers)

    #fetch the slave node to which this req should be redirected
    def find_slave_node(self,hxmd5):
	server = self.ring.get_node(hxmd5)
	return server

    #calculate md5 hash for the given key
    def calculatemd5(self,key):
	m = hashlib.md5()
	m.update(key)
	return m.digest()

    def listen(self):
	print 'listening....' 
	while True:
			  try:
			   	  request, addr = self.sock.recvfrom(1024)			  
				  rec_req = json.loads(request)
				  rec_req['id'] = 1
				  logger.debug('Processing {0} request from userid= {1},data={2}'.format(rec_req['id'],rec_req['userid'],rec_req['data']))		
				  try:
					#rec_req['data'].keys():
					key = rec_req['data'].keys()[0]
				  except:
					key = rec_req['data']
				  print rec_req
				  hxmd5 = self.calculatemd5(key)		  
				  slave_node = self.find_slave_node(hxmd5)
				  print slave_node
				  #rec_req['md5'] = "".join(ord(x) for x in hxmd5)
                                  rec_req['md5'] = "hello"
				  rec_req['Master'] = 'Master'+str(masternum)
				  print rec_req
			  except:
				print (traceback.format_exc())
			  
                          #fetch slave node host and port
			  host,port = slave_node.partition(":")[::2]
			  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		    	  print host,port
		    	  sock.sendto(json.dumps(rec_req), (host,int(port)))
		          logger.debug('Processed {0} request, sent to {1}'.format(rec_req['id'],slave_node))
		    	  sock.close()
			  if rec_req['request'] == 'get':
			  	self.send_to_neighbours(slave_node,rec_req)

    #If request is get then we need to fetch the result from 2 neighbouring nodes of the selected slave node.
    def send_to_neighbours(self,slave_node,rec_req):
		for name,endpoint in self.slave_nodes.items():
			if endpoint == slave_node:
				slave = name
		slave_num=int(slave[5:])
		count = len(self.memcache_servers)
		#send it to two neighbours
		slave2 = ((slave_num%count)+1)
		slave3 = ((slave_num%count)+2)
                if slave2 > count:
			slave2 = slave2%count
		if slave3 > count:
			slave3 = slave3%count
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		host,port = self.slave_nodes['Slave'+str(slave2)].partition(":")[::2]
		sock.sendto(json.dumps(rec_req), (host,int(port)))
		host,port = self.slave_nodes['Slave'+str(slave3)].partition(":")[::2]
		sock.sendto(json.dumps(rec_req), (host,int(port)))
		sock.close()
		
	
if __name__ == "__main__":    
	s=MasterMonitorConnection(10003)
	s.listen()
