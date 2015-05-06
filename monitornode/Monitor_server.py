import sys
import socket
import sqlite3
import select 
import sys
import threading
import thread
from threading import Thread, Lock
from multiprocessing.pool import ThreadPool
import json

number_of_masters = 2
masters = dict()
clients = dict()
threshold = {'Master1':'No','Master2':'No'}
i = 0
receivestatus_port = 10008

def thresholdListen():
	'''listen for threshold from the masters and update threshold dictionary.this will be used will redirecting requests to master.'''
	print 'Threshold daemon running'
	portNumber = 10007
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)         
	host = socket.gethostname()               
	sock.bind((host, portNumber))
	while True:
   		request, addr = sock.recvfrom(1024)
		master,val = request.split(" ")
		threshold[master]= val

def informslaves(data):
	req = {"request":"New","data":data}
	with open("config/slave.txt",'r') as fin:
		for line in fin:
			host,port = line.strip().split("=")[1].split(",")[0].split(":")
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sock.sendto(json.dumps(data), (host,int(port)))
			sock.close()
		
	
def receiveStatus():
	'''get the status for processed request from master and send it back to client'''
	global masters
	print 'Listening for status on port ' + str(receivestatus_port)
	portNumber = receivestatus_port
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)         
	host = socket.gethostname()               
	sock.bind((host, portNumber))
	while True:
   		status, addr = sock.recvfrom(1024)
		returnobj=json.loads(status)
		if returnobj['result'] == "shutdown":
			masters = {k:v for k,v in masters.items if v.split(":")[0] != addr[0]}
		elif returnobj['result'] == "New":
			data = "Master{}={}:{}".format(len(masters),returnobj['host'],returnobj['port'])
			with open("config/masters.txt",'a') as myfile:
				myfile.write("{}\n".format(data))
			with open("config/slave.txt",'r') as fin:
				slaves = ""
				for line in fin:
					slaves = slaves + line	
				sock.sendto(slaves)
			informslaves(data)
			masters["Master{}".format(len(masters))] = "{}:{}".format(returnobj['host'],returnobj['port'])
		else:
			clients[returnobj['userid']].send(str(returnobj['result'])) 


class Server:
	''' Accept requests from clients and redirect them to masters in round robin pattern'''
	def __init__(self): 
		self.host = ''
        	self.port = 13464
        	self.backlog = 5
        	self.size = 1024
        	self.server = None
		self.pool = ThreadPool(10)
		with open("config/masters.txt") as myfile:
			for line in myfile:
				name, endpoint = line.partition("=")[::2]
				masters[name] = endpoint
		#name = "Master1"
		#endpoint = "localhost:10003"
			
	def open_socket(self):

		self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.server.bind((self.host,self.port))
		self.server.listen(5)
		
	def run(self): 
		self.open_socket()
		input = [self.server,] 
		running = 1 
		
		#start a daemon thread to listen threshold
		thread = Thread(target = thresholdListen, args = ())
		thread.start()
	
		#start a thread to listen results of req from masters
		resultThread = Thread(target = receiveStatus, args = ())
		resultThread.start()
	
		while running: 
	   		inputready,outputready,exceptready = select.select(input,[],[])                
	  		for s in inputready:
				if s == self.server:
					
					client,address = self.server.accept()
					#Assign one thread to handle each client.
		                        self.pool.apply_async(run, args=(client,address))
				else:
					junk = sys.stdin.readline()
		                        running = 0 

		self.server.close()


def getMaster():
	'''This method will return the next master to which the request should be redirected.'''
	global i
	i=((i)%(number_of_masters))+1
	while True:
		if threshold['Master'+str(i)] == 'Yes':
			i=((i)%(number_of_masters))+1
		else:
			break
	return 'Master'+str(i)


def run(client,address):
	'''This method will be run in seperate thread to process client requests.'''
        size = 1024
        running = 1
	attempts = 0
	flag = 0
        while running:
	    while(attempts < 3 and flag == 0):
		attempts = attempts + 1
	        data = json.loads(client.recv(size))
            	if data:
		    clients[data['username']]=client
		    conn_2 = sqlite3.connect('authentication_info.db')
	   	    c_2 = conn_2.cursor()
	   	    password = ""
	   	    for row in c_2.execute("SELECT password from user_info where username = '%s'" % data['username']):
	   		password = row
	   	    conn_2.close()
	   	    if not password:
			client.send('Login failed')
	   	    elif data['password'] != password[0]:
			client.send('Login failed')
		    else:
	   		print 'Login Successful\n'
	   		client.send('Thank you for connecting')
			flag = 1
			break
	    request_key_value_pair = json.loads(client.recv(size))
	    if (request_key_value_pair['request']=="Logout"):
		print "closing connection"
		client.close()
	        running = 0 
		flag = 0
	    print 'Request is ' + str(request_key_value_pair)
	    master_node = getMaster()
	    print master_node+'is serving the request'
	    host,port = masters[master_node].partition(":")[::2]
	    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	    print host,port
	    sock.sendto(json.dumps(request_key_value_pair), (host,int(port)))
	    sock.close()
	    #client.send("successfully received input data and request")


if __name__ == "__main__":

	conn = sqlite3.connect('authentication_info.db')
	c = conn.cursor()
	#c.execute('''DROP TABLE user_info''')
	c.execute("CREATE TABLE user_info (username text, password text)")
	c.execute("INSERT INTO user_info values('shashank','goud')")
	c.execute("INSERT INTO user_info values('ankit','bhandari')")
	c.execute("INSERT INTO user_info values('kaustubh','sant')")
	c.execute("INSERT INTO user_info values('nikhil','chintapallee')")
	conn.commit()
	conn.close()
	s = Server()
	s.run() 
