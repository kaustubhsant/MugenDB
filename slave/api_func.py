import string
import fileinput
import json

class MugenDBAPI:
    def __init__(self):
        self.readbuff = {}
        self.dbfile = "MugenDBfile.txt"
        self.keymapfile = "KeyMap.txt"

    def mark_key(self,infile,seekpoint):
	newline = ""
    	with open(infile,'r+') as fwr:
	   	fwr.seek(seekpoint,0)
        	line = fwr.readline()
		print line
		newline = "!" 
		newline = line[0:2] + newline + line[3:]
		print "marked " + newline
   		fwr.seek(seekpoint,0)
        	fwr.write(newline.strip())
    
    def deleteline(self,infile,key):
	import fileinput
	file = fileinput.input(infile, inplace=True)
	for line in file:
     	    if key not in line:
       		print line

    def put(self,data,keylocation,userid):
        for key in data.keys():
            if key in keylocation:
                print "Error: key '{0}' already exists".format(key)
                return -1
            else:
		with open(self.keymapfile,'ab+') as keyfin:
                    with open(self.dbfile,'ab+') as dbf:
                        dbf.seek(0,2)
                        offset = dbf.tell()
                    	json.dump(data,dbf)
                    	dbf.write("\n")
                    	keylocation[key] = [userid,offset]
			keyfin.write("{0}:{1}\n".format(key,keylocation[key]))
                    	print "Success: Added '{0}'".format(data)
                    	return 0

    def get(self,key,keylocation,userid):
        if key in keylocation:
            '''
            For access control policy
            if userid != keylocation[key][0]:
                return "Error: No access to key '{0}'".format(key)
            '''
            with open(self.dbfile,'rb') as dbf:
                dbf.seek(keylocation[key][1],0)
                self.readbuff = dbf.readline()
                return json.loads(self.readbuff)
        else:
            print "Error: Key '{0}' does not exists".format(key)
            return -1
	        
    def update(self,data,keylocation,userid):
        for key in data.keys():
            if key in keylocation:
                '''
                For access control policy
                if userid != keylocation[key][0]:
                return "Error: No access to key '{0}'".format(key)
                '''
                seekpoint = keylocation[key][1]
                self.mark_key(self.dbfile,seekpoint)
		with open(self.keymapfile,'ab+') as keyfin:
	            with open(self.dbfile,'ab+') as dbf:
                    	dbf.seek(0,2)
                    	offset = dbf.tell()
                    	json.dump(data,dbf)
                    	keylocation[key][1] = offset 
			keyfin.write("{}:{}\n".format(key,keylocation[key]))
                    	print "Success: Updated key '{0}' with '{1}'".format(key,data[key])
                    	return 0
            else:
            	print "Error: Key {0} does not exists".format(key)
            	return -1

    def delete(self,key,keylocation,userid):
        if key in keylocation:
            '''
            For access control policy
            if userid != keylocation[key][0]:
                return "Error: No access to key '{0}'".format(key)
            '''
            seekpoint = keylocation[key][1]
            self.mark_key(self.dbfile,seekpoint)
            keylocation.pop(key,None)
	    self.deleteline(self.keymapfile,key)
            print "Success: Deleted key '{0}'".format(key)
            return 0
        else:
            print "Error: Key '{0}' does not exists".format(key)
            return -1


if __name__ == "__main__":
    
    ''' For testing above code'''
    
    userid = "kaustubh"
    data = {'name':'kaustubh'}
    keylocation = {}
    dbcaller = MugenDBAPI()
    dbcaller.put(data, keylocation, userid)
    print "After put " + str(keylocation)
    dbcaller.put({'address':'raleigh'}, keylocation, userid)
    print "After put " + str(keylocation)
    dbcaller.put({'address':'new york'}, keylocation, userid)
    print dbcaller.get('address', keylocation, userid)
    data = {'name':'ankit'}
    dbcaller.update(data, keylocation, userid)
    print keylocation
    print dbcaller.get('name', keylocation, userid)
    print dbcaller.get('address', keylocation, userid)
    dbcaller.delete('name',keylocation,userid)
    print keylocation
    
