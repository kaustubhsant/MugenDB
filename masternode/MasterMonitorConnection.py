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
from threading import Thread

masternum = 1
#intialize logging
log_filename = 'logs/'+'slave-log.txt'
logging.basicConfig(filename = log_filename,filemode = 'a',level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('MasterMonitorConnection.py')
backup_slave_nodes = {'Slave1':'152.46.17.96:10000','Slave2':'152.46.17.115:10001','Slave3':'152.1.13.84:10002','Slave4':'152.46.20.248:10003'}

class MasterMonitorConnection:
    ''' Receive the requests from monitor node and redirect them to slave nodes using consistent hashing'''
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
	self.backup = dict()
	self.config = {"result":"New","host":self.host,"port":self.portNumber}
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	host,port = self.monitors[self.monitors.keys()[0]].split(":")
	sock.sendto(json.dumps(self.config), (host,int(port)))
	slave_config = sock.recvfrom(1024)
	sock.close()
	with open("config/slave.txt",'w') as fin:
		for val in slave_config:
			fin.write(val)

	with open("config/slave.txt",'r') as myfile:
		for line in myfile:
			name, endpoints = line.partition("=")[::2]
			endpoint1,endpoint2= endpoints.split(',')
			self.memcache_servers.append(endpoint1)
			self.slave_nodes[name]=endpoint1
			self.backup[name] =endpoint2
	self.ring = HashRing(self.memcache_servers)
	print self.backup

    def find_slave_node(self,hxmd5):
	'''fetch the slave node to which this req should be redirected'''
	server = self.ring.get_node(hxmd5)
	return server

    def calculatemd5(self,key):
	'''calculate md5 hash for the given key'''
	m = hashlib.md5()
	m.update(key)
	return m.digest()

    def listen(self):
	''' listen and redirect the requests to slaves '''
	print 'listening....' 
	thread = Thread(target = self.receive_slave_failure, args = ())
	thread.start()
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

    def send_to_neighbours(self,slave_node,rec_req):
	'''If request is get then we need to fetch the result from 2 neighbouring nodes of the selected slave node.'''
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
	host,port = self.backup['Slave'+str(slave2)].partition(":")[::2]
	sock.sendto(json.dumps(rec_req), (host,int(port)))
	host,port = self.backup['Slave'+str(slave3)].partition(":")[::2]
	sock.sendto(json.dumps(rec_req), (host,int(port)))
	sock.close()

    def receive_slave_failure(self):
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)         
	host = socket.gethostname()  
	print 'receiving failure'             
	sock.bind((self.host, 10012))
	global backup_slave_nodes
	while True:
		request, addr = sock.recvfrom(1024)
		#request will be just the slave number
		dead_slaves = json.loads(request)
		print 'Recieved dead slaves : '
		print dead_slaves
		for slave in dead_slaves:
			if slave in self.slave_nodes.keys():
				endpoint = self.slave_nodes[slave]
				del self.slave_nodes[slave]
				del self.backup[slave]
				self.memcache_servers.remove(endpoint)
				print 'after deletion'+str(self.slave_nodes)
				#Now add the backup slave nodes
				self.slave_nodes[slave]=backup_slave_nodes[slave]
				self.backup[slave]=backup_slave_nodes[slave]
				self.memcache_servers.append(backup_slave_nodes[slave])
				print 'after addition'+str(self.slave_nodes)
				#change the ring using the new slave
				self.ring = HashRing(self.memcache_servers)
					
	
if __name__ == "__main__":    
	s=MasterMonitorConnection(10003)
	s.listen()
