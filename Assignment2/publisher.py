import sys
import zmq
import time
from random import randrange

time.sleep(0.5)

my_ip  = sys.argv[1] 
M      = int(sys.argv[2]) if len(sys.argv)>2 else 3
srv_ip = '10.0.0.1'
MOD = 2**M

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://*:5555")


def register(): 
    reg_sock = context.socket(zmq.REQ)
    conn_str = "tcp://" + srv_ip + ":5556"
    print "I am a new publisher "
    reg_sock.connect(conn_str)
    reg_sock.send('REG ' + my_ip)
    msg = reg_sock.recv()  #succer of my_key
    reg_sock.disconnect(conn_str)
    print "reg finished:",msg


def publish():
  evt_key = randrange(1, MOD+1) #rr(a,b) => [a,b)
  temp = randrange(-80, 135)
  socket.send_string("%i %s %i %f" % (evt_key, my_ip, temp, time.time()))


register()
while True: 

  publish()

  time.sleep(0.05)

