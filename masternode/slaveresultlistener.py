import socket
from datetime import time
import json	
import logging
import time
import traceback
import os

#intialize logging
log_filename = 'logs/'+'slave-log.txt'
logging.basicConfig(filename = log_filename,filemode = 'a',level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('slaveresultlistener.py')

class SlaveListener:
    ''' Class to setup listening port and receive results
	and forward to monitor'''

  
    def __init__(self,portNumber):
        self.portNumber = portNumber
	self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)         
	self.host = socket.gethostname()               
	self.sock.bind((self.host, self.portNumber)) 
	self.monitors = dict()	
        with open("config/monitor.txt") as myfile:
            for line in myfile:
                name, endpoint = line.partition("=")[::2]
                self.monitors[name] = endpoint

    def listen(self):
	print 'listening....' #TODO : log this
	while True:
   	    req, addr = self.sock.recvfrom(1024)
	    request = json.loads(req)
	    logger.debug('Recieved request id {} with result {}'.format(request['id'],request['result']))
	    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	    host,port = self.monitors[self.monitors.keys()[0]].split(":")
	    print host,port,str(request)
	    sock.sendto(json.dumps(request), (host,int(port)))
	    sock.close()
	    logger.debug('sent request id {} with result {} to monitor'.format(request['id'],request['result']))
			

if __name__ == "__main__":    
	s=SlaveListener(12600)
	s.listen()
