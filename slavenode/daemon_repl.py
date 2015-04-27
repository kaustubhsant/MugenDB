import os.path
import subprocess
import os

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
		os.system("sshpass -p '{0}' scp Backup_MugenDBfile.patch backup@{1}:/home/backup".format(self.replpass[0][1],self.replpass[0][0]))
		os.system("sshpass -p '{0}' scp Backup_MugenDBfile.patch backup@{1}:/home/backup".format(self.replpass[1][1],self.replpass[1][0]))
		os.system("sshpass -p '{0}' scp Backup_MugenDBfile.patch backup@{1}:/home/backup".format(self.replpass[2][1],self.replpass[2][0]))
		return 0
		

if __name__ == "__main__":
	bkp = Backup_Data()
	bkp.backup()
	print "taking backup is completed"
