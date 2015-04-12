import sys
import socket
import sqlite3
import select 
import sys
import threading
import thread
from multiprocessing.pool import ThreadPool
import json

class Server:
	def __init__(self): 
		self.host = ''
        	self.port = 13464
        	self.backlog = 5
        	self.size = 1024
        	self.server = None
		self.pool = ThreadPool(10)
#        	self.threads = [] 
	def open_socket(self):

		self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.server.bind((self.host,self.port))
		self.server.listen(5)
		
	def run(self): 
		self.open_socket()
		input = [self.server,] 
		running = 1 
		while running: 
	   		inputready,outputready,exceptready = select.select(input,[],[])                
	  		for s in inputready:
				if s == self.server:
					
					client,address = self.server.accept()
#                		        c.start()
		                        self.pool.apply_async(run, args=(client,address))
				else:
					junk = sys.stdin.readline()
		                        running = 0 

		self.server.close()
#	        for c in self.threads:
 #       	    c.join() 

def run(client,address):
	
#	client = client1
#        address = address1
        size = 1024
        running = 1
	attempts = 0
        while running:
	    while(attempts < 3):
		attempts = attempts + 1
	        data = json.loads(client.recv(size))
            	if data:
		    conn_2 = sqlite3.connect('authentication_info.db')
	   	    c_2 = conn_2.cursor()
	   	    password = ""
	   	    for row in c_2.execute("SELECT password from user_info where username = '%s'" % data['username']):
	   		password = row
	   	    conn_2.commit()
	   	    conn_2.close()
	   	    if not password:
			client.send('Login failed')
	   	    elif data['password'] != password[0]:
			client.send('Login failed')
		    else:
	   		print 'Login Successful\n'
	   		client.send('Thank you for connecting')
			break
	    request_key_value_pair = json.loads(client.recv(size))
	    if (request_key_value_pair['request']=="Logout"):
		print "closing connection"
		client.close()
	        running = 0 
	    print 'Request is ' + str(request_key_value_pair)
	    client.send("successfully received input data and request")


if __name__ == "__main__":

	conn = sqlite3.connect('authentication_info.db')
	c = conn.cursor()
#	c.execute('''DROP TABLE user_info''')
	c.execute("CREATE TABLE user_info (username text, password text)")
	c.execute("INSERT INTO user_info values('shashank','goud')")
	c.execute("INSERT INTO user_info values('ankit','bhandari')")
	c.execute("INSERT INTO user_info values('kaustubh','sant')")
	c.execute("INSERT INTO user_info values('nikhil','chintapallee')")
	conn.commit()
	conn.close()
	s = Server()
	s.run() 
