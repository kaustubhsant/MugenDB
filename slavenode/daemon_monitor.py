import socket
import sys
import time
import json

class Monitor:
    def __init__(self,host,portnumber,isTcp):
	self.isTcp = isTcp
	if self.isTcp == 'True':    
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.host = socket.gethostbyname_ex(host)
		self.portno = int(portnumber)
		self.s.connect((self.host,self.portno))
	else:
		self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.host = host
		self.portno = int(portnumber)
    
    def gatherdata(self):
        data = ""
        return data
    
    def heartbeat(self):
        return "Alive"
    
    def senddata(self,data):
	if self.isTcp == 'True':
        	self.s.send(data)
	else:
		self.s.sendto(data, (self.host,self.portno))
        
    def closeconnection(self):
        self.s.close()
    

def main(args):
	conndetails = dict()
	conndetails['host'] = args[1]
	conndetails['port'] = args[2]
        with open("config/monitor.txt") as myfile:          
            for line in myfile:
                monitor_ip,monitor_port = line.strip().split("=")[1].split(":")
	m = Monitor(monitor_ip,monitor_port,False)
	m.senddata(json.dumps(conndetails))
	while(1):
		m.senddata("Alive")
		time.sleep(2)
	
if __name__ == "__main__":
	main(sys.argv)
	