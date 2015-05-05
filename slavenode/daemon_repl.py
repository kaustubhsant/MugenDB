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
		''' Creates threads and does scp for data replication to  nodes'''


		if(os.path.isfile(self.backupfile) == False):
			f = file(self.backupfile,"w")
		os.system("diff -u {0} {1} > {2}".format(self.backupfile,self.dbfile,self.backuppatch))
		os.system("patch {0} {1}".format(self.backupfile,self.backuppatch))
		t1 = threading.Thread(target=self.repl,args=(self.replpass[0],))
		t2 = threading.Thread(target=self.repl,args=(self.replpass[1],))
		t3 = threading.Thread(target=self.repl,args=(self.replpass[2],))
		t1.start()
		t2.start()
		t3.start()
		t1.join()
		t2.join()
		t3.join()
		os.system("rm {}".format(self.backuppatch))
		return 0
	
	def repl(self,repl):
		logger.debug("replicating on {0}@{1}".format(repl[0],repl[1]))
		os.system("cp Backup_MugenDBfile.patch Backup_MugenDBfile_{}.patch".format(repl[0]))
		os.system("sshpass -p '{0}' scp Backup_MugenDBfile_{1}.patch backup_slave@{2}:/home/backup_slave/backup".format(repl[2],repl[0],repl[1]))
		os.system("rm Backup_MugenDBfile_{}.patch".format(repl[0]))
		logger.debug("replicated on {0}@{1}".format(repl[0],repl[1]))
		

if __name__ == "__main__":
	bkp = Backup_Data()
	bkp.backup()
	print "taking backup is completed"
