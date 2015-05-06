import sys
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
import os
from daemon_monitor import Monitor
from threading import Thread

#intialize logging
log_filename = 'logs/'+'slave-log.txt'
logging.basicConfig(filename = log_filename,filemode = 'a',level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('MasterSlaveConnection.py')
keylocation = {}
num_of_requests = 0
threshold = 2000

class MasterSlaveConnection:
    ''' Class to setup listening port and receive requests
	and assign a thread from threadpool to process request'''

  
    def __init__(self,portNumber):
        self.portNumber = portNumber
	self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)         
	self.host = socket.gethostname()               
	self.sock.bind((self.host, self.portNumber)) 
        self.masters = dict()
	self.monitors = dict()
        with open("config/masters.txt") as myfile:
            for line in myfile:
                name, endpoint = line.partition("=")[::2]
                self.masters[name] = endpoint
	
        with open("config/monitor.txt") as myfile:
            for line in myfile:
                name, endpoint = line.partition("=")[::2]
                self.monitors[name] = endpoint
	print self.monitors
        self.pool = ThreadPool(10) #TODO : configure this

    def listen(self):
	print 'listening....' #TODO : log this
	#self.pool.apply_async(savekeymap,args=(self.keylocation,))
	thread = Thread(target = self.sendHeartbeat, args = ())
	thread.start()
	while True:
   	    request, addr = self.sock.recvfrom(1024)
	    if req['request'] == "New":
		with open("config/masters.txt",'a') as fin:
			fin.write(req['data'])
			fin.write("\n")
		self.masters[data.split("=")[0]] = data.split("=")[1]
	    else:
	    	self.pool.apply_async(ServeRequest, args=(request,self.masters,keylocation))
	    global num_of_requests
	    num_of_requests=num_of_requests+1
	    if num_of_requests > threshold:
	       host,port = self.monitors[getMonitor()].partition(":")[::2]
               mon = Monitor(host,int(port),'False')
               mon.senddata('threshold')
               mon.closeconnection

    def sendHeartbeat(self):
	while True:
		time.sleep(2) #sends heartbeat every 2 secs
		print 'sending heartbeat'
		host,port = self.monitors[getMonitor()].partition(":")[::2]
		mon = Monitor(host,int(port),'False')
		mon.senddata('Heartbeat')

def getMonitor():
	return 'Monitor1'

def ServeRequest(request,masters,keylocation):
	''' Process the request and return result '''


    	try:
		req = json.loads(request)
		requestid = req['id']
		masterNode = req['Master']
                userid = req['userid']
                action = req['request']
		data = req['data']
		
		#call apis here	
		logger.debug('Processing {0} request from master {1},userid= {2},data={3}'.format(requestid,masterNode,userid,data))
		api=MugenDBAPI()
		if action == "put":
		    val = api.put(data,keylocation,userid)
		elif action == "get":
		    val=api.get(data,keylocation,userid)
		elif action == "update":
		    val=api.update(data,keylocation,userid)
		elif action == "delete":
		    val=api.delete(data,keylocation,userid)
	
		logger.debug('Processing {0} request from master {1},userid= {2},data={3},return={4}'.format(requestid,masterNode,userid,data,val))
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		host,port = masters[masterNode].partition(":")[::2]
		print host,port,val
		res = dict()
		res['id'] = requestid
		res['userid'] =  userid
		res['result'] =  val
		res['request'] = action
		sock.sendto(json.dumps(res), (host,int(port)))
		sock.close()
                global num_of_requests
                num_of_requests=num_of_requests-1
	except:
		print (traceback.format_exc())

def loadkeymap():
	'''Load the keymap from file '''


	global keylocation 
	if os.path.isfile("KeyMap.txt"):
		with open("KeyMap.txt",'r') as keyfin:
			for line in keyfin:
				keylocation[line.strip().split(":")[0]] = line.strip().split(":")[1]


def main(args):
	port = int(args[1])
	loadkeymap()
	s=MasterSlaveConnection(port)
	s.listen()

if __name__ == "__main__":    
	main(sys.argv)
