import sys

import hashlib

import getpass

import socket

def main(argv):

    if len(argv) != 1:

        sys.exit('Usage: user_pass.py <file_name>')

 

    print '\nUser & Password Authentication Program v.01\n'


    try:

        file_conn = open(sys.argv[1])

        user_name = file_conn.readline()[:-1]

        password = file_conn.readline()[:-1]

        file_conn.close()

    except:

        sys.exit('There was a problem reading the file!')

         

    pass_try = 0

    x = 3

     

    if raw_input('Please Enter User Name: ') != user_name:

        sys.exit('Incorrect User Name, terminating... \n')

     

    while pass_try < x:

#        user_input = hashlib.sha224(getpass.getpass('Please Enter Password: ')).hexdigest()

         user_input = raw_input('Please Enter Password:')
         
         if user_input != password:

            pass_try += 1

            print 'Incorrect Password, ' + str(x-pass_try) + ' more attemts left\n'

         else:

            pass_try = x+1

             

    if pass_try == x and user_input != password:

        sys.exit('Incorrect Password, terminating... \n')

 

    print 'Login Successful\n'

#    choice = raw_input('Enter your request: \n')
    
 #  print 'Your choice is ' + choice
          
    s = socket.socket()
    host = socket.gethostname();
    port = 13465
#    s.bind((host,port))
    s.connect((host, port))
    print s.recv(1024)
    s.close          

if __name__ == "__main__":

    main(sys.argv[1:])
