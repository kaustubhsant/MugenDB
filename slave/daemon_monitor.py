import socket

class Monitor:
    def __init__(self,host,portnumber):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.host = socket.gethostbyname_ex(host)
        self.portno = portnumber
        self.s.connect((self.host,self.portno))
    
    def gatherdata(self):
        data = ""
        return data
    
    def heartbeat(self):
        return "Alive"
    
    def senddata(self,data):
        self.s.send(data)
        
    def closeconnection(self):
        self.s.close()
    