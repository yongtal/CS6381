import sys
import zmq
import time

time.sleep(0.9)

srv_ip = '10.0.0.1' 
zipo = sys.argv[1]
my_key = int(zipo)
slp = int(sys.argv[2]) if len(sys.argv) > 2 else 0
my_ip = sys.argv[3] if len(sys.argv) > 3 else "" 

context = zmq.Context()
socket = context.socket(zmq.SUB)
poller = zmq.Poller()

reg_socket = context.socket(zmq.REQ)
connect_str = "tcp://" + srv_ip + ":5556"
reg_socket.connect(connect_str)

zip_filter = zipo
if isinstance(zip_filter, bytes):
    zip_filter = zip_filter.decode('ascii')

def register():
  NotFind = True
  while NotFind:
    print "finding my succer..."
    reg_socket.send('FTS ' + str(my_key))
    msg = reg_socket.recv()
    key, succ_ip = msg.split(' ')
    if key == -1: #didn't find, sth wrong
      time.sleep(5) #wait sometime
    else:
      NotFind = False
    
  print "my succ is: ",key,succ_ip
  print "sub to my succer"
  connect_str = "tcp://" + succ_ip + ":5557"
  socket.connect(connect_str)
  socket.setsockopt_string(zmq.SUBSCRIBE, zip_filter)
  poller.register(socket, zmq.POLLIN)
  return succ_ip

def subscribe():
  if poller.poll(10000):
    msg = socket.recv()
    evtk, tmp, sendtime, rst =  msg.split(' ',3)
    duration = time.time() - float(sendtime)
    print evtk, tmp, duration, rst 
    return True
  else :
    return False
  
#ask for history
def history():
  if (slp > 0):
    hnum = 5 
    time.sleep(slp)
    print "The time is :", time.ctime()
    his_socket = context.socket(zmq.REQ)
    his_socket.connect("tcp://" + succ_ip + ":5556")
    his_socket.send('HST ' + str(my_key) + ' ' + str(hnum)) 
    msg = his_socket.recv()
    print "Reciving last #", hnum, " history samples of key", my_key," from weather server..."
    print msg
    his_socket.close()
    print

succ_ip = register()
history()

print("Collecting updates from weather server...")
while True:
  if not subscribe() :
    socket.disconnect("tcp://"+succ_ip+":5557")
    poller.unregister(socket) 
    socket.close()
    time.sleep(5)
    socket = context.socket(zmq.SUB)
    succ_ip = register()       #re_register to server 
