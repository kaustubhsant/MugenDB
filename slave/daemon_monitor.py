import socket

class Monitor:
    def __init__(self,host,portnumber,isTcp):
	self.isTcp = isTcp
	if self.isTcp == 'True':    
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.host = socket.gethostbyname_ex(host)
		self.portno = portnumber
		self.s.connect((self.host,self.portno))
	else:
		self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.host = host
		self.portno = portnumber
    
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
    
