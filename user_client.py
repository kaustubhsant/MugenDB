import sys

import hashlib

import getpass

import socket

user_name = raw_input('Please enter username: ')
user_password = raw_input('Please Enter Password:')
user_details = user_name + ',' + user_password
s = socket.socket()
host = socket.gethostname();
port = 13464
s.connect((host, port))
s.send(user_details)
print s.recv(1024)
request = raw_input('Enter your request: ')
if str(request) =="put" :
	user_input=raw_input("Enter key and value: ")
if str(request)=="delete" :
	user_input=raw_input("Enter key: ")
if str(request)=="update" :
	user_input=raw_input("Enter key and value: ")
if str(request)=="get" :
	user_input=raw_input("Enter key: ")

input_to_monitor = request+ '@' + user_input
s.send(input_to_monitor)
print s.recv(1024)
s.close          

