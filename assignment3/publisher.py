import sys
import zmq 
import time 
from random import randrange

myip = "10.0.0."+sys.argv[1]
servers = ['10.0.0.1','10.0.0.2','10.0.0.3']
wtime = 3000
tpcrange = 5

context = zmq.Context()
socket = context.socket(zmq.REQ)

def connect():
  srv_ip = servers[randrange(len(servers))]
  conn_str = "tcp://" + srv_ip + ":5556"
  socket.connect(conn_str)

  while True:
    socket.setsockopt(zmq.RCVTIMEO, wtime)
    socket.setsockopt(zmq.LINGER, 0)
    #request leader ip
    socket.send('leader ' + 'hi')
    try:
      msg = socket.recv()
      print "Found new leader",msg
      socket.disconnect(conn_str)
      return msg 
    except zmq.error.Again:
      print "No reply from ",conn_str
      try:
        servers.remove(srv_ip)
      except ValueError:
        pass
 

#connect to leader
leader_ip = connect()
conn_str = "tcp://" + leader_ip + ":5556"
socket.connect(conn_str)

#socket.setsockopt(zmq.RCVTIMEO, 2000)
#socket.setsockopt(zmq.LINGER, 0)

def publish():
  time.sleep(0.5)
  topic = randrange(tpcrange)
  value = randrange(-10,90)
  hval = (topic+1)**3*int(sys.argv[1])**2
  power = hash(str(hval)+myip) % 256 

  socket.send_string("pub %i %i %i %s %f" %(topic, power, value, myip, time.time()))
  try:
    msg = socket.recv()
    print "rcv msg:", msg
    return True
  except zmq.error.Again:
    print "No reply from ",conn_str
    return False 


while True:
  if not publish():
    socket.disconnect(conn_str)
    socket.close()
    time.sleep(3)

    try:
      servers.remove(leader_ip)
    except ValueError:
      pass
  
    socket = context.socket(zmq.REQ)
 
    leader_ip = connect()
    conn_str = "tcp://" + leader_ip + ":5556"
    socket.connect(conn_str)

