import sys
import zmq 
import time 
from random import randrange

myip = "10.0.0."+sys.argv[1]
hist = int(sys.argv[2]) if len(sys.argv) > 2 else 0
servers = ['10.0.0.1','10.0.0.2','10.0.0.3']
wtime = 3000
subtime = 15000
tpcrange = 5

context = zmq.Context()

def connect():
  socket = context.socket(zmq.REQ)

  srv_ip = servers[randrange(len(servers))]
  conn_str = "tcp://" + srv_ip + ":5556"
  socket.connect(conn_str)

  socket.setsockopt(zmq.RCVTIMEO, wtime)
  socket.setsockopt(zmq.LINGER, 0)

  while True:
    #request leader ip
    socket.send('leader ' + 'hi')
    try:
      msg = socket.recv()
      print "Found new leader",msg
      socket.disconnect(conn_str)
      socket.close()
      return msg 
    except zmq.error.Again:
      print "No reply from ",conn_str
      try:
        servers.remove(srv_ip)
      except ValueError:
        pass
 
topic = str(randrange(tpcrange)) 
if isinstance(topic, bytes):
    topic = topic.decode('ascii')

leader_ip = connect()
conn_str = "tcp://" + leader_ip+ ":5557"
sub_socket = context.socket(zmq.SUB)
sub_socket.connect(conn_str)
sub_socket.setsockopt(zmq.RCVTIMEO, subtime)
sub_socket.setsockopt(zmq.LINGER, 0)
sub_socket.setsockopt_string(zmq.SUBSCRIBE, topic)

#ask for history
def history(hist):
  if (hist > 0):
    hist = 5 
    time.sleep(hist)
    #print "The time is :", time.ctime()
    his_socket = context.socket(zmq.REQ)
    his_socket.connect("tcp://" + leader_ip + ":5556")
    his_socket.send('HST ' + str(topic) + ' ' + str(hist)) 
    msg = his_socket.recv()
    print "Reciving last #", hist , " history samples of topic", topic," from weather server..."
    print msg
    his_socket.close()
    print

def subscribe():
  try:
    msg = sub_socket.recv()
    tpc,val,stime,lip,pip,pwr = msg.split(' ',5)
    dur = time.time() - float(stime)
    print "rcv msg:",tpc, val, dur, lip, pip, pwr 
    return True
  except zmq.error.Again:
    print "No reply from ",conn_str
    sub_socket.close()
    return False 

history(hist)

print("Collecting updates from weather server...")
while True:
  if not subscribe():
    #sub_socket.disconnect(conn_str)
    sub_socket.close()
    try:
      servers.remove(leader_ip)
    except ValueError:
      pass

    leader_ip = connect()
    conn_str = "tcp://" + leader_ip + ":5557"
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(conn_str)
    sub_socket.setsockopt(zmq.RCVTIMEO, subtime)
    sub_socket.setsockopt(zmq.LINGER, 0)
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, topic)
