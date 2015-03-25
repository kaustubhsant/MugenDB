import socket
import sqlite3

conn = sqlite3.connect('authentication_info.db')
c = conn.cursor()
c.execute('''DROP TABLE user_info''')
c.execute("CREATE TABLE user_info (username text, password text)")
c.execute("INSERT INTO user_info values('shashank','goud')")
c.execute("INSERT INTO user_info values('ankit','bhandari')")
c.execute("INSERT INTO user_info values('kausthub','sant')")
c.execute("INSERT INTO user_info values('nikhil','chintapallee')")
conn.commit()
conn.close()
               
s = socket.socket()         
host = socket.gethostname() 
port = 13464               
s.bind((host, port))        

s.listen(5)                 
while True:
   c,addr = s.accept()     
   print 'Got connection from', addr

   data = c.recv(1024)
   user_info = data.split(",")
   username_input = user_info[0]
   password_input = user_info[1]
   conn_2 = sqlite3.connect('authentication_info.db')
   c_2 = conn_2.cursor()
   for row in c_2.execute("SELECT username from user_info where username = '%s'" % username_input):
   	username = row
   for row in c_2.execute("SELECT password from user_info where username = '%s'" % username_input):
   	password = row
   conn_2.commit()
   conn_2.close()
   username_split = '(u\'' + username_input + '\',)'
   password_split = '(u\'' + password_input + '\',)'
   if str(username_split) != str(username):
        sys.exit('Incorrect User Name, terminating... \n')
   if str(password_split) != str(password):
        sys.exit('Incorrect Password, terminating... \n')

   print 'Login Successful\n'

   c.send('Thank you for connecting')
   request = c.recv(1024)
   if str(request) == 'insert':
   	c.send('Enter the key and value to insert')
   if str(request) == 'delete':
   	c.send('Enter the key to delete')
   if str(request) == 'update':
   	c.send('Enter the key to update')
   key_value_pair = c.recv(1024)
   key_value = key_value_pair.split('#')
   print 'received key is ' + key_value[0]
   print 'received value is ' + key_value[1]
   c.close()  