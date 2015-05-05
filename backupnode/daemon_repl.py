import os
import logging
import time

log_filename = 'logs/'+'repl.log'
logging.basicConfig(filename = log_filename,filemode = 'a',level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('daemon_repl.py')

class Backup_Data:
	def __init__(self):
		self.backupfile = ""
		self.backuppatch = ""
		with open("config/replnode.txt",'r') as fin:
			for line in fin:
				self.backupfile = "Backup_MugenDBfile_{}.txt".format(line.strip())
				self.backuppatch = "Backup_MugenDBfile_{}.patch".format(line.strip())			

	def repl(self):
		if(os.path.isfile(self.backuppatch) == True):		
			if(os.path.isfile(self.backupfile) == False):
				f = file(self.backupfile,"w")
			logger.debug("patching file")
			os.system("patch {} {}".format(self.backupfile,self.backuppatch))
			logger.debug("patched file")			
			os.system("rm {}".format(self.backuppatch))	

	
if __name__ == "__main__":
	bkp = Backup_Data()
	while(1):
		bkp.repl()
		time.sleep(1)	
