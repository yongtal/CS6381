import zmq
import time
import Queue

context = zmq.Context()

#sock used for publsher registration & recv events 
#also for sending histroy to suber 
reg_sock = context.socket(zmq.REP)
connect_str = "tcp://*:5556"
reg_sock.bind(connect_str)
    
#publish events from evt srv to subers
socket = context.socket(zmq.PUB)
socket.bind("tcp://*:5557")

MIN = 10000
MAX = 10100
hnum = 5

Onr = {}
Con = {}

#init history table
Hst = {}
for i in range (MIN, MAX+1):
  Hst[i] = Queue.Queue(hnum) 
  for j in range (hnum):
    Hst[i].put("None")


def registered(ip):
  return ip in Onr 

def un_reg(ip):
  Onr.pop(ip, None)
  update_onr(ip)

def reg(ip):
  Onr[ip] = (0,1)
  update_onr(ip)

def update_onr(ip):
  size = len(Onr)
  totl = MAX - MIN + 1
  intrv = totl / size + 1
  i = 0
  for key in Onr:
    Onr[key] = (MIN + intrv*i, MIN + intrv*(i+1))
    i += 1

def pub_own_topic(ip, topic):
  top = int(topic)
  if ip not in Onr :
    un_reg(ip)
    return False
  return Onr[ip][0] <= top and Onr[ip][1] > top

def update_Hst(zipo, smsg):
  Hst[int(zipo)].get()
  Hst[int(zipo)].put(smsg)

def get_Hst(zipo, num):
  his = ""
  num = min(hnum, num)
  for i in range(num) :
    ll = list(Hst[int(zipo)].queue)
    his = his + ll[num-1-i] + ' \n'  
  return his

def update_Conn(ip, tsp):
  Con[ip] = tsp

def check_Conn():
  cur = time.time()
  for ip in Con:
    if cur - Con[ip] > 3 : #hvn't pub in 3 secs, assume died 
      Con.pop(ip, None)
      un_reg(ip)
      print "Un registered", ip 
      print "Current ownership"
      print Onr
      break

  return cur
  
last_check = time.time()

while True:
  strr = reg_sock.recv()
  print " ", 
  reply = "OK"
  typ,msg = strr.split(' ',1)
  if typ == 'pub' and not registered(msg) :
    #a new publihser came in, assign owner_strgenth to him
    reg(msg)
    print "registered", msg
    print "Current ownership"
    print Onr
      
  elif typ == 'sub' :
    ip, zipo, num = msg.split()
    reply = get_Hst(int(zipo), int(num))

  elif typ == 'evt' :
    #publish this event 
    ip, zipo, rst = msg.split(' ', 2)
    if pub_own_topic(ip, zipo) :
      smsg = rst + " " + ip
      socket.send_string(zipo +" " + smsg)
      update_Hst(zipo, smsg)

      tpr, rhel, tsp = rst.split()
      update_Conn(ip, float(tsp))
   
  reg_sock.send(reply)

  #check connection with puber every 10 secs
  if time.time() - last_check > 10 : 
    last_check = check_Conn()

   
