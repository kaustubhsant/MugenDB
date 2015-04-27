import os
import threading
import logging

log_filename = 'logs/'+'repl.log'
logging.basicConfig(filename = log_filename,filemode = 'a',level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('daemon_repl.py')

class Backup_Data:
	def __init__(self):
		self.dbfile = "MugenDBfile.txt"
		self.backupfile = "Backup_MugenDBfile.txt"
		self.backuppatch = "Backup_MugenDBfile.patch"
		self.replpass = list()
		with open("config/password.txt",'r') as fin:
			for line in fin:
				self.replpass.append(line.strip().split("="))

	def backup(self):		
		if(os.path.isfile(self.backupfile) == False):
			f = file(self.backupfile,"w")
		os.system("diff -u {0} {1} > {2}".format(self.backupfile,self.dbfile,self.backuppatch))
		os.system("patch {0} {1}".format(self.backupfile,self.backuppatch))
		t1 = threading.Thread(target=self.repl,args=(self.replpass[0][1],self.replpass[0][0],))
		t2 = threading.Thread(target=self.repl,args=(self.replpass[1][1],self.replpass[1][0],))
		t3 = threading.Thread(target=self.repl,args=(self.replpass[2][1],self.replpass[2][0],))
		t1.start()
		t2.start()
		t3.start()
		t1.join()
		t2.join()
		t3.join()
		return 0
	
	def repl(self,passwd,node):
		logger.debug("replicating on {}".format(node))
		os.system("sshpass -p '{0}' scp Backup_MugenDBfile.patch backup@{1}:/home/backup".format(passwd,node))
		logger.debug("replicated on {}".format(node))
		

if __name__ == "__main__":
	bkp = Backup_Data()
	bkp.backup()
	print "taking backup is completed"
