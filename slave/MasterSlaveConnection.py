import socket
import threading
import thread
from multiprocessing.pool import ThreadPool
from datetime import time
import json

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
        self.pool = ThreadPool(10) #TODO : configure this
        self.keylocation = {}

    def listen(self):
	print 'listening....' #TODO : log this
	self.pool.apply_async(savekeymap,args=(self.keylocation,))
	while True:
   	    data, addr = self.sock.recvfrom(1024)
    	    self.pool.apply_async(ServeRequest, args=(data,self.masters,))


def ServeRequest(data,masters):
	print data
        masterNode,request = data.partition(" ")[::2]
        #call apis here
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        host,port = masters[masterNode].partition(":")[::2]
	sock.sendto('Success', (host,int(port)))
        sock.close()

def savekeymap(keylocation=None):
    keymapfile = "KeyMap.txt"
    while True:
        with open(keymapfile,'w') as kf:
            json.dump(keylocation,kf)
        time.sleep(10)
    
s=MasterSlaveConnection(12345)
s.listen()
