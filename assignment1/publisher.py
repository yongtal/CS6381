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
#   Weather update server
#   Binds PUB socket to tcp://*:5556
#   Publishes random weather updates
#
import sys
import zmq
import time
from random import randrange

my_ip = sys.argv[1] 
srv_ip = '10.0.0.1'

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://" + srv_ip + ":5556")

socket.send('pub ' + my_ip) 

msg = socket.recv()

# The difference here is that this is a publisher and its aim in life is
# to just publish some value. The binding is as before.

t0 = time.time()

#assume host with ip 0.2 will die after 15 seconds
def go_on():
  if my_ip == '10.0.0.2' and time.time() - t0 > 10 :
    return False
  return True

# keep publishing 
while go_on():
  #zipcode = randrange(1, 100000)
  zipcode = randrange(10000, 10100)
  temperature = randrange(-80, 135)
  relhumidity = randrange(10, 60)
  socket.send_string("evt %s %i %i %i %f" % (my_ip, zipcode, temperature, relhumidity, time.time()))
  msg = socket.recv()

#host will quit
#3socket.send('qit ' + my_ip)
#3msg = socket.recv()
  
