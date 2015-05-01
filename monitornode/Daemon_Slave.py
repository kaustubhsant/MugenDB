import socket
import time
import datetime
from threading import Thread

nodes = {'Slave1':datetime.datetime.now(),'Slave2':datetime.datetime.now(),'Slave3':datetime.datetime.now(),'Slave4':datetime.datetime.now(),'Master1':datetime.datetime.now(),'Master2':datetime.datetime.now()}
time_for_dead = 15  #15 seconds

class DaemonSlave:
	def __init__(self,portNumber):
		self.portNumber = portNumber
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)         
		self.host = socket.gethostname()               
		self.sock.bind((self.host, self.portNumber))

	def listen(self):
		print 'listening....'
		#listen for heartbeat from slaves
		while True:
   	  		request, addr = self.sock.recvfrom(1024)
			nodes[request]=datetime.datetime.now()
			print request

	#this method is used restart dead slave nodes from which we didnt hear any heartbeat for preconfigured time.
	def run(self):
		thread = Thread(target = self.listen, args = ())
		thread.start()
		while True:
			time.sleep(20)
			dead = self.get_dead_nodes()
			print 'printing dead...'
			print str(dead)

	# return an array of dead slave nodes. If we don receive an heart beat for 'time_for_dead' seconds, we assume that slave is dead.
	def get_dead_nodes(self):
		silent = [slave for (slave, hbtime) in nodes.items() if (datetime.datetime.now()-hbtime).seconds > time_for_dead]
		return silent


if __name__ == "__main__":    
	s=DaemonSlave(12345)
	s.run()
