import socket
import time
import datetime
from threading import Thread
import json

nodes = {'Slave1':datetime.datetime.now(),'Slave2':datetime.datetime.now(),'Slave3':datetime.datetime.now(),'Slave4':datetime.datetime.now()}
time_for_dead = 15  #15 seconds

class DaemonSlave:
	''' Class to listen heartbeat from slave nodes and invoke vm scripts on dead slaves'''
	def __init__(self,portNumber):
		self.portNumber = portNumber
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)         
		self.host = socket.gethostname()               
		self.sock.bind((self.host, self.portNumber))
		self.slave_nodes = dict()
		self.masters = dict()
		with open("config/slave.txt",'r') as myfile:
			for line in myfile:
				name, endpoint1 = line.partition("=")[::2]
				self.slave_nodes[name]=endpoint1

		with open("config/masters.txt") as myfile:
			for line in myfile:
				name, endpoint = line.partition("=")[::2]
				self.masters[name] = endpoint

	def listen(self):
		'''listen for heartbeat from slaves'''
		print 'listening....'
		while True:
			print 'listening....'
   	  		request, addr = self.sock.recvfrom(1024)
			print addr
			print self.slave_nodes
			slave = ''
			#get slave nodes heartbeat
			for name,endpoint in self.slave_nodes.items():
				if endpoint.split(":")[0] == addr[0]:
					slave = name
					print 'got heartbeat'
			if slave!= '':
				nodes[slave]=datetime.datetime.now()
			print request

	def run(self):
		'''this method is used restart dead slave nodes from which a heartbeat hasn't been heared for preconfigured time.'''
		thread = Thread(target = self.listen, args = ())
		thread.start()
		while True:
			time.sleep(time_for_dead)
			dead = self.get_dead_nodes()
			print 'printing dead...'
			self.update_masters(dead)
			print str(dead)

	def get_dead_nodes(self):
		'''return an array of dead slave nodes. If we don receive an heart beat for 'time_for_dead' seconds,then slave is dead.'''
		silent = [slave for (slave, hbtime) in nodes.items() if (datetime.datetime.now()-hbtime).seconds > time_for_dead]
		return silent
	
	def update_masters(self,dead):
		for master,endpoint in self.masters.items():
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			host,port = endpoint.split(':')[0],10012
			print 'in update_masters'
			print host,port
			sock.sendto(json.dumps(dead), (host,int(port)))	


if __name__ == "__main__":    
	s=DaemonSlave(10015)
	s.run()
