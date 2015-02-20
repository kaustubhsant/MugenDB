import socket
from multiprocessing import Process,Pool

class MasterSlaveConnection:
    def __init__(self,portNumber):
        self.portNumber = portNumber
	self.sockConn = socket.socket()         
	self.host = socket.gethostname()               
	self.sockConn.bind((self.host, self.portNumber)) 
	self.pool = Pool(processes=10) #make this configurable

    def listen(self):
	print 'I am listening'
        self.sockConn.listen(5)               
	while True:
   	    c, addr = self.sockConn.accept()     
	    self.pool.map(ProcessClient(c))

    
def ProcessClient(c):
	while True:
            data = c.recv(1024)
            if data:
	        print data +'\n' #call api here
		#how to kill the process
	    
#s=MasterSlaveConnection(12345)
#s.listen()
