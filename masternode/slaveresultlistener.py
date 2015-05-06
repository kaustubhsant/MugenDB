import socket
from datetime import time
import json	
import logging
import time
import traceback
import os
from threading import Thread
from time import sleep
import datetime

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
	self.results = dict()
	#req_times is used to push the results of get req when there is a delay in fetching them from multiple nodes.
	self.req_times = dict()
	
        with open("config/monitor.txt") as myfile:
            for line in myfile:
                name, endpoint = line.partition("=")[::2]
                self.monitors[name] = endpoint


    def listen(self):
	'''listen for responses from slaves and redirect them back to monitor node'''
	print('listening....') #TODO : log this
	thread = Thread(target = self.push_results, args = ())
	thread.start()
	while True:
   	    req, addr = self.sock.recvfrom(1024)
	    request = json.loads(req)
	    logger.debug('Recieved request id {} with result {}'.format(request['id'],request['result']))
	    if request['request']!='get':
		    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		    host,port = self.monitors[self.monitors.keys()[0]].split(":")
		    print host,port,str(request)
		    sock.sendto(json.dumps(request), (host,int(port)))
		    sock.close()
	    else:
		    #take note of the time at which listner got initial get req
                    if request['id'] not in self.req_times: 
			    self.req_times[request['id']]=datetime.datetime.now()
		    #append the returned results of get req. Based on the majority we would be chosing a winner.
		    if request['id'] in self.results:
		    	    self.results[request['id']].append(request)
		    else:
			    self.results[request['id']] = [request]
                    
		    json_list=self.results[request['id']]
		    if len(json_list) >= 3:
			    majority_result= self.get_majority(json_list)
		    	    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			    host,port = self.monitors[self.monitors.keys()[0]].split(":")
			    sock.sendto(json.dumps(majority_result), (host,int(port)))
			    sock.close()
			    #delete the items related to this req.
			    del self.results[request['id']]
			    del self.req_times[request['id']]
	    logger.debug('sent request id {} with result {} to monitor'.format(request['id'],request['result']))
    
    def get_majority(self,json_list):
	'''return the result which is a majority among the given list'''
	temp_dict = {}
        for res in json_list:
               if res['result']!=-1:
	           key,val = (res['result'].items())[0]
	       else:
		   key,val = '-1','-1'
	       if val in temp_dict:
		   temp_dict[val].append(res)
	       else:
 		   temp_dict[val]=[res]
        majority_result = [obj for (res1,obj) in temp_dict.items() if len(obj) >= 2]
        return majority_result[0][0]
  
    def push_results(self):
	'''this method is to push the updates of get requests which are being held back for more than 500 millisec'''
	while True:
		sleep(0.005) #sleep for 5 milli sec
		silent_reqs = [reqid for (reqid, hbtime) in self.req_times.items() if (datetime.datetime.now()-hbtime).seconds > 0.005]
		if len(silent_reqs) > 0 :
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			host,port = self.monitors[self.monitors.keys()[0]].split(":")
			logger.debug('Silent requests :'+str(silent_reqs))
			for reqid in silent_reqs:
				logger.debug('pushing results'+ str(reqid))
				sock.sendto(json.dumps(self.results[int(reqid)][0]), (host,int(port)))
				#delete the items related to this req.
                                del self.req_times[reqid]
				del self.results[int(reqid)]
			sock.close()		

    def shutdown(self):
	result = {"result":"shutdown"}
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	host,port = self.monitors[self.monitors.keys()[0]].split(":")
	sock.sendto(json.dumps(result), (host,int(port)))
	sock.close()


if __name__ == "__main__":    
	s=SlaveListener(12600)
	try:
		s.listen()
	except KeyboardInterrupt:
		s.shutdown()
