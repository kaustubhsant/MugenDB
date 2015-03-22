import socket
import threading
import thread
from multiprocessing.pool import ThreadPool
from datetime import time
import json	
from api_func import MugenDBAPI

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

    def listen(self):
	print 'listening....' #TODO : log this
	self.pool.apply_async(savekeymap,args=(self.keylocation,))
	while True:
   	    request, addr = self.sock.recvfrom(1024)
    	    self.pool.apply_async(ServeRequest, args=(request,self.masters,))


def ServeRequest(request,masters):
        masterNode,userid,action,data = request.split(" ")
        #call apis here	
        api=MugenDBAPI()
	if action == "put":
	    val = api.put(data,userid)
	elif action == "get":
	    val=api.get(data,userid)
	elif action == "update":
	    val=api.update(data,userid)
	elif action == "delete":
	    val=api.delete(data,userid)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        host,port = masters[masterNode].partition(":")[::2]
	sock.sendto(val, (host,int(port)))
        sock.close()

def savekeymap(keylocation=None):
    keymapfile = "KeyMap.txt"
    while True:
        with open(keymapfile,'w') as kf:
            json.dump(keylocation,kf)
        time.sleep(10)
    
s=MasterSlaveConnection(12345)
s.listen()
