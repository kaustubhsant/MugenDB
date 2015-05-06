import os
import threading
import logging
import time

log_filename = 'logs/'+'repl.log'
logging.basicConfig(filename = log_filename,filemode = 'a',level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('daemon_repl.py')

class Backup_Data:
	def __init__(self):
		self.dbfile = "MugenDBfile.txt"
		self.backupfile = "Backup_MugenDBfile.txt"
		self.backuppatch = "Backup_MugenDBfile.patch"
		self.keymapfile = "KeyMap.txt"
		self.keymapbackup = "Backup_KeyMap.txt"
		self.keymappatch = "Backup_KeyMap.patch"
		self.replpass = list()
		with open("config/password.txt",'r') as fin:
			for line in fin:
				self.replpass.append(line.strip().split("="))

	def backup(self):		
		''' Creates threads and does scp for data replication to  nodes'''


		if(os.path.isfile(self.backupfile) == False):
			f = file(self.backupfile,"w")
		os.system("diff -u {0} {1} > {2}".format(self.backupfile,self.dbfile,self.backuppatch))
		if(os.path.isfile(self.keymapbackup) == False):
			f = file(self.keymapbackup,"w")
		os.system("diff -u {0} {1} > {2}".format(self.keymapbackup,self.keymapfile,self.keymappatch))
		os.system("sshpass -p '{0}' scp {1} spulima@{2}:/home/spulima/backup".format(self.replpass[0][2],self.keymappatch,self.replpass[0][1]))
                os.system("patch {0} {1}".format(self.keymapbackup,self.keymappatch))
		os.system("rm {}".format(self.keymappatch))
		t1 = threading.Thread(target=self.repl,args=(self.replpass[0],))
		t2 = threading.Thread(target=self.repl,args=(self.replpass[1],))
		t3 = threading.Thread(target=self.repl,args=(self.replpass[2],))
		t1.start()
		t2.start()
		t3.start()
		t1.join()
		t2.join()
		t3.join()
                os.system("patch {0} {1}".format(self.backupfile,self.backuppatch))
		os.system("rm {}".format(self.backuppatch))
		return 0
	
	def repl(self,repl):
		logger.debug("replicating on {0}@{1}".format(repl[0],repl[1]))
		os.system("cp Backup_MugenDBfile.patch Backup_MugenDBfile_{}.patch".format(repl[0]))
		os.system("sshpass -p '{0}' scp Backup_MugenDBfile_{1}.patch spulima@{2}:/home/spulima/backup".format(repl[2],repl[0],repl[1]))
		logger.debug("replicated on {0}@{1}".format(repl[0],repl[1]))
		os.system("rm Backup_MugenDBfile_{}.patch".format(repl[0]))

	def repl_neighbor(self):
		filename = "/home/spulima/backup/Backup_MugenDBfile_{0}.patch".format(self.replpass[3][0])
		print filename
		if(os.path.isfile(filename) == True):		
			if(os.path.isfile("MugenDBfile_{0}.txt".format(self.replpass[3][0])) == False):
				f = file("MugenDBfile_{0}.txt".format(self.replpass[3][0]),"w")
			logger.debug("patching file on {0}@{1}".format(self.replpass[3][0],self.replpass[3][1]))
			os.system("patch MugenDBfile_{0}.txt {1}".format(self.replpass[3][0],filename))
			logger.debug("patched file on {0}@{1}".format(self.replpass[3][0],self.replpass[3][1]))			
			os.system("rm {}".format(filename))	

	
if __name__ == "__main__":
	bkp = Backup_Data()
	while(1):
		bkp.backup()
		bkp.repl_neighbor()
		time.sleep(1)	
