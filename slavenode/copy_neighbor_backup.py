import os
import threading
import logging
import time
from api_func import MugenDBAPI
from hash_ring import HashRing

log_filename = 'logs/'+'repl.log'
logging.basicConfig(filename = log_filename,filemode = 'a',level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('copy_neighbor_backup.py')

class Redistribute:
	def __init__(self,newnode):
		self.slavenum = "Slave1"
		self.newnode = newnode
		self.replpass = list()
		with open("config/password.txt",'r') as fin:
			for line in fin:
				self.replpass.append(line.strip().split("="))
	def split_keys(self,keylocation):
		keys_1= {}
		keys_2= {}
		api=MugenDBAPI('temp_dbfile','temp_keyfile')
		for_new = MugenDBAPI('MugenDBfile_{}.txt'.format(self.newnode[0]),'KeyMap_{}.txt'.format(self.newnode[0]))
		servers = [self.slavenum+":10000",self.newnode[1]+":10000"]
		ring = HashRing(servers)
		for key in keylocation:
			with open("MugenDBfile.txt",'r') as myfile:
				myfile.seek(keylocation[key][1],0)
				data = json.loads(myfile.readline())
			hxmd5=calculatemd5(key)
			server=ring.get_node(hxmd5)
			if server == servers[1]:
				for_new.put(key,data,keys_2,keylocation[key][0])
			else:
				api.put(key,data,keys_1,keylocation[key][0])

		os.system('mv temp_dbfile.txt MugenDBfile.txt ')
		os.system('mv temp_keyfile.txt KeyMap.txt ')

	def repl_addition(self,keylocation):
		os.system("sshpass -p '{0}' scp Backup_MugenDBfile_{1}.txt spulima@{2}:/home/spulima/backup".format(self.replpass[-1][2],self.replpass[-1][0],self.newnode[1]))
		os.system("sshpass -p '{0}' scp Backup_MugenDBfile_{1}.txt spulima@{2}:/home/spulima/backup".format(self.replpass[-2][2],self.replpass[-2][0],self.newnode[1]))
		# call distribute keys
		self.split_keys(keylocation)
		os.system("sshpass -p '{0}' scp MugenDBfile_{1}.txt spulima@{2}:/home/spulima/backup".format(self.newnode[2],self.newnode[0],self.newnode[1]))	
		os.system("cp MugenDBfile_{0}.txt Backup_MugenDBfile_{0}.txt".format(self.newnode[0]))
		# copy to new backup node
		os.system("sshpass -p '{0}' scp Backup_MugenDBfile_{1}.txt spulima@{2}:/home/spulima/backup".format(self.newnode[2],self.newnode[0],self.replpass[1][1]))
		self.replpass[-2] = self.newnode
		#update password.txt
		with open("config/password.txt",'w') as fin:
			for val in self.replpass:
				fin.write("{0}={1}={2}\n".format(val[0],val[1],val[2]))


	
if __name__ == "__main__":
	bkp = Redistribute(["Slave5","152.46.12.11","ABCabc123"])
	keylocation = {}
	while(1):
		bkp.repl_addition(keylocation)
		time.sleep(1)	
