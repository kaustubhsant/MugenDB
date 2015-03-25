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
request = raw_input('Enter your request ')
s.send(request)
print s.recv(1024)
key = raw_input('Key: ')
value = raw_input('Value: ')
key_value_pair = key + '#' + value
s.send(key_value_pair)
s.close          

