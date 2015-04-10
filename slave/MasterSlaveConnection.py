import socket
import threading
import thread
from multiprocessing.pool import ThreadPool
from datetime import time
import json	
from api_func import MugenDBAPI
import logging
import time
import traceback

#intialize logging
log_filename = 'logs/'+'slave-log.txt'
logging.basicConfig(filename = log_filename,filemode = 'a',level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('MasterSlaveConnection.py')
keylocation = {}

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
                self.masters[name] = endpoint
        myfile.close()
        self.pool = ThreadPool(10) #TODO : configure this

    def listen(self):
	print 'listening....' #TODO : log this
	#self.pool.apply_async(savekeymap,args=(self.keylocation,))
	while True:
   	    request, addr = self.sock.recvfrom(1024)
    	    self.pool.apply_async(ServeRequest, args=(request,self.masters,keylocation))


def ServeRequest(request,masters,keylocation):
    	try:
		masterNode,userid,action,data = request.split(" ")
		#call apis here	
		logger.debug('Processing '+action+' request from master '+masterNode+' ,userid='+userid+' ,data='+data)
		api=MugenDBAPI()
		if action == "put":
		    jsondata = {data.split(":")[0]:data.split(":")[1]}
		    val = api.put(jsondata,keylocation,userid)
		elif action == "get":
		    val=api.get(data,keylocation,userid)
		elif action == "update":
		    jsondata = {data.split(":")[0]:data.split(":")[1]}
		    val=api.update(data,keylocation,userid)
		elif action == "delete":
		    val=api.delete(data,keylocation,userid)
	
		logger.debug('Processed succesfully: '+action+' request from master '+masterNode+' ,userid='+userid+' ,data='+data+' ,return= '+str(val))
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		host,port = masters[masterNode].partition(":")[::2]
		print host,port,val
		sock.sendto(str(val), (host,int(port)))
		sock.close()
	except:
		print (traceback.format_exc())

if __name__ == "__main__":    
	s=MasterSlaveConnection(12345)
	s.listen()
