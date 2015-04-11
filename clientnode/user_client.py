import sys
import socket
import json

def request_input(request):
	if str(request) =="put" :
		userinput = {}
		data = raw_input("Enter data as key:value\n")
		userinput[data.split(":")[0]] = data.split(":")[1]
		input_to_monitor['data'] = userinput
	elif str(request)=="get" :
		userinput=raw_input("Enter key: ")
		input_to_monitor['data'] = userinput
	elif str(request)=="update" :
		userinput = {}
		data = raw_input("Enter data as key:value\n")
		userinput[data.split(":")[0]] = data.split(":")[1]
		input_to_monitor['data'] = userinput
	elif str(request)=="delete" :
		userinput=raw_input("Enter key: ")
		input_to_monitor['data'] = userinput
	else:
		print 'wrong input'
		return 1
	return 0


user_details ={}
input_to_monitor = {}
attempts = 0 
username = raw_input('Please enter username: ')
userpassword = raw_input('Please Enter Password:')
user_details['username'] = username
user_details['password'] = userpassword

s = socket.socket()
host = socket.gethostname();
port = 13464
s.connect((host, port))
s.send(json.dumps(user_details))

while(attempts <= 3):
	attempts = attempts + 1 
	if s.recv(1024) == "Login failed" :
	    username = raw_input('Please enter username:')
	    userpassword = raw_input('Please Enter Password:')
	    user_details['username'] = username
	    user_details['password'] = userpassword
	    s.send(json.dumps(user_details))
	else:
	    break

if attempts >3:
    sys.exit(1)
	
request = raw_input('Enter your request:')

result = request_input(request)
while result:
	request = raw_input('Enter your request:')
	result = request_input(request)

input_to_monitor['userid'] = username
input_to_monitor['request'] = request

s.send(json.dumps(input_to_monitor))
print s.recv(1024)
s.close()          


