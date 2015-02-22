import socket
import threading
import thread
from multiprocessing import Pool


class MasterSlaveConnection:
    def __init__(self,portNumber):
        self.portNumber = portNumber
	self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)         
	self.host = socket.gethostname()               
	self.sock.bind((self.host, self.portNumber)) 
        self.masters = dict()
        with open("config/masters.txt") as myfile:
            for line in myfile:
                name, endpoint = line.partition("=")[::2]
                print endpoint
                self.masters[name] = endpoint
        myfile.close()
        
    def listen(self):
	print 'listening....' #TODO : log this
	while True:
   	    data, addr = self.sock.recvfrom(1024)
            if threading.activeCount() < 10 : #make this configurable
	        thread.start_new_thread(ServeRequest,(data,self.masters))
            
    
def ServeRequest(data,masters):
	print data
        masterNode,request = data.partition(" ")[::2]
        #call apis here
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        host,port = masters[masterNode].partition(":")[::2]
	sock.sendto('Success', (host,int(port)))
        sock.close()
    
#s=MasterSlaveConnection(12345)
#s.listen()
