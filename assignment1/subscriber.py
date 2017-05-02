# Sample code for CS6381
# Vanderbilt University
# Instructor: Aniruddha Gokhale
#
# Code taken from ZeroMQ examples with additional
# comments or extra statements added to make the code
# more self-explanatory  or tweak it for our purposes
#
# We are executing these samples on a Mininet-emulated environment
#
#


#
#   Weather update client
#   Connects SUB socket to tcp://localhost:5556
#   Collects weather updates and finds avg temp in zipcode
#

import sys
import zmq
import time

srv_ip = '10.0.0.1' 
my_ip = sys.argv[1] 
zipo = sys.argv[2]
slp = int(sys.argv[3]) if len(sys.argv) > 3 else 0


#  Socket to talk to server
context = zmq.Context()

# Since we are the subscriber, we use the SUB type of the socket
socket = context.socket(zmq.SUB)

# Here we assume publisher runs locally unless we
# send a command line arg like 10.0.0.1
connect_str = "tcp://" + srv_ip + ":5557"

socket.connect(connect_str)

# Subscribe to zipcode, default is NYC, 10001
zip_filter = zipo

# Python 2 - ascii bytes to unicode str
if isinstance(zip_filter, bytes):
    zip_filter = zip_filter.decode('ascii')

# any subscriber must use the SUBSCRIBE to set a subscription, i.e., tell the
# system what it is interested in
socket.setsockopt_string(zmq.SUBSCRIBE, zip_filter)


#asking for history
hnum = 5 
if (slp > 0):
  time.sleep(slp)
  print "The time is :", time.ctime()
  context = zmq.Context()
  his_socket = context.socket(zmq.REQ)
  his_socket.connect("tcp://" + srv_ip + ":5556")
  his_socket.send('sub ' + my_ip + ' ' +  zipo + ' ' + str(hnum)) 
  msg = his_socket.recv()
  print "Reciving last #", hnum, " history samples of zipcode", zipo," from weather server..."
  print msg
  his_socket.close()
  print

print("Collecting updates from weather server...")

while True:
  string = socket.recv_string()
  zipo, temp, rhul, tsp, src = string.split()
  ntn = time.time() - float(tsp)
  print zipo, temp, src, ntn

