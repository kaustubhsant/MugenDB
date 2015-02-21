import socket
from multiprocessing import Pool

class MasterSlaveConnection:
    def __init__(self,portNumber):
        self.portNumber = portNumber
	self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)         
	self.host = socket.gethostname()               
	self.sock.bind((self.host, self.portNumber)) 
	self.pool = Pool(processes=10) #make this configurable

    def listen(self):
	print 'I am listening'
	while True:
   	    data, addr = self.sock.recvfrom(1024) 
	    [self.pool.apply_async(ProcessClient, args=(data,))]
            print 'returned'
    
def ProcessClient(data):
	print data #call apis here
	    
#s=MasterSlaveConnection(12345)
#s.listen()
