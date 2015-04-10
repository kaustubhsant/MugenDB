import socket
import time
from threading import Thread

slaves = {'Slave1':time.time(),'Slave2':time.time(),'Slave3':time.time(),'Slave4':time.time()}
time_for_dead = 15  #15 seconds

class DaemonSlave:
	def __init__(self,portNumber):
		self.portNumber = portNumber
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)         
		self.host = socket.gethostname()               
		self.sock.bind((self.host, self.portNumber))

	def listen(self):
		print 'listening....' #TODO : log this
		while True:
   	  		request, addr = self.sock.recvfrom(1024)
			slaves[request]=time.time()
			print request

	def run(self):
		thread = Thread(target = self.listen, args = ())
		thread.start()
		while True:
			time.sleep(2)
			dead = self.get_dead_slaves()
			print 'printing dead...'
			print str(dead)
		
	def get_dead_slaves(self):
		silent = [slave for (slave, hbtime) in slaves.items() if time.time()-hbtime > time_for_dead]
		return silent


if __name__ == "__main__":    
	s=DaemonSlave(12345)
	s.run()
