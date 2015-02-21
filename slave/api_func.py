import string
import fileinput
import json

class MugenDBAPI:
	def __init__(self):
		self.readbuff = {}
		self.dbfile = "MugenDBfile.txt"

	def put(self,data,keylocation,userid):
		for key in data.keys():
			if key in keylocation:
				print "Error: key {0} already exists".format(key)
				return -1
			else:
				with open(self.dbfile,'ab+') as dbf:
					dbf.seek(0,2)
					offset = dbf.tell()
					json.dump(data,dbf)
					dbf.write("\n")
					keylocation[key] = [userid,offset]
					print "Success: Added {0}".format(data)
					return 0

	def get(self,key,keylocation,userid):
		if key in keylocation:
			'''
			For access control policy
			if userid != keylocation[key][0]:
				return "Error: No access to key {0}".format(key)
			'''
			with open(self.dbfile,'rb') as dbf:
				dbf.seek(keylocation[key][1],0)
				self.readbuff = dbf.readline()
				return json.loads(self.readbuff)
		else:
			print "Error: Key {0} does not exists".format(key)
			return -1

	def deleteline(self,infile,seekpoint,keylocation):
		with open(infile,'r') as fre:
			fre.seek(seekpoint,0)
			fre.readline()
			with open(infile,'r+') as fwr:
				fwr.seek(seekpoint,0)
				line = fre.readline()
				while(line):
					offset = fwr.tell()
					key = json.loads(line).keys()[0]
					fwr.write(line)
					keylocation[key][1] = offset
					line = fre.readline()
				fwr.truncate()

	def update(self,data,keylocation,userid):
		for key in data.keys():
			if key in keylocation:
				'''
				For access control policy
				if userid != keylocation[key][0]:
				return "Error: No access to key {0}".format(key)
				'''
				seekpoint = keylocation[key][1]
				self.deleteline(self.dbfile,seekpoint,keylocation)
				with open(self.dbfile,'ab+') as dbf:
					dbf.seek(0,2)
					offset = dbf.tell()
					json.dump(data,dbf)
					keylocation[key][1] = offset 
					print "Success: Updated {0} with {1}".format(key,data[key])
					return 0
		else:
			print "Error: Key {0} does not exists".format(key)
			return -1

	def delete(self,key,keylocation):
		if key in keylocation:
			'''
			For access control policy
			if userid != keylocation[key][0]:
				return "Error: No access to key {0}".format(key)
			'''
			seekpoint = keylocation[key][1]
			self.deleteline(self.dbfile,seekpoint,keylocation)
			keylocation.pop(key,None)
			print "Success: Deleted key {0}".format(key)
			return 0
		else:
			print "Error: Key {0} does not exists".format(key)
			return -1

if __name__ == "__main__":
	
	''' For testing above code'''
	
	userid = "kaustubh"
	data = {'name':'kaustubh'}
	keylocation = {}
	dbcaller = MugenDBAPI()
	dbcaller.put(data, keylocation, userid)
	print keylocation
	dbcaller.put({'address':'raleigh'}, keylocation, userid)
	print keylocation
	print dbcaller.get('address', keylocation, userid)
	data = {'name':'ankit'}
	dbcaller.update(data, keylocation, userid)
	print keylocation
	print dbcaller.get('name', keylocation, userid)
	dbcaller.delete('name',keylocation)
	print keylocation
	
	