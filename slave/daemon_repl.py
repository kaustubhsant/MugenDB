import os.path
import subprocess
import os

class Backup_Data:
	def __init__(self):
		self.dbfile = "MugenDBfile.txt"
		self.backupfile = "Backup_MugenDBfile.txt"
		self.backuppatch = "Backup_MugenDBfile.patch"

	def backup(self):		
		if(os.path.isfile(self.backupfile) == False):
			f = file(self.backupfile,"w")
		os.system("diff -u {} {} > {}".format(self.backupfile,self.dbfile,self.backuppatch))
		os.system("patch {} {}".format(self.backupfile,self.backuppatch))
		#os.system("scp Backup_MugenDBfile.patch username@backupnode:/home/backup")
		return 0
		

if __name__ == "__main__":
	bkp = Backup_Data()
	bkp.backup()
	print "taking backup is completed"
