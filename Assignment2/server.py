import sys 
import zmq
import time
import Queue
from node import Node
from random import randrange
import funcs as fc

m_key = int(sys.argv[1])
m_ip  = sys.argv[2]
M     = int(sys.argv[3]) if len(sys.argv) > 3 else 3
MOD = 2**M
NUM = 4

my = Node(m_key, m_ip)
succ = Node(m_key, m_ip)
prcd = Node()
finger = [Node()]*M 
suclst = [Node()]*3

#e.g. mmod(4,4) = 4 not 0
def mmod(val, mm = MOD):
  return mm if val%mm == 0 else val%mm

def init_scl():
  suclst[0]=Node(mmod(my.key+2), '10.0.0.'+str(mmod(my.key/2+1,NUM)))
  suclst[1]=Node(mmod(my.key+4), '10.0.0.'+str(mmod(my.key/2+2,NUM)))
  suclst[2]=Node(mmod(my.key+6), '10.0.0.'+str(mmod(my.key/2+3,NUM)))
  print "succer list:"
  for i in range(3):
    print suclst[i].key, suclst[i].ip

def init_fgr():
  finger[0]=Node(mmod(my.key+2), '10.0.0.'+str(mmod(my.key/2+1,NUM)))
  finger[1]=Node(mmod(my.key+2), '10.0.0.'+str(mmod(my.key/2+1,NUM)))
  finger[2]=Node(mmod(my.key+4), '10.0.0.'+str(mmod(my.key/2+2,NUM)))
  succ.key = mmod(my.key+2)
  succ.ip = '10.0.0.'+str(mmod(my.key/2 + 1,NUM))
  prcd.key = mmod(my.key-2)
  prcd.ip = '10.0.0.'+str(mmod(my.key/2-1,NUM))
  print "my   key, ip:",my.key,my.ip
  print "succ key, ip:",succ.key,succ.ip
  print "prcd key, ip:",prcd.key,prcd.ip
  print "finger table:"
  for i in range(len(finger)):
    print finger[i].key,finger[i].ip


context = zmq.Context()
reg_sock = context.socket(zmq.REP)
connect_str = "tcp://*:5556"
reg_sock.bind(connect_str)

socket = context.socket(zmq.PUB)
socket.bind("tcp://*:5557")

poller = zmq.Poller()
poller.register(reg_sock, zmq.POLLIN)


#Notice request can have no reply: mssg is empty!
def request(ip,typ,msg,wtime = 1000):
  msg = str(msg)
  req_socket = context.socket(zmq.REQ)
  conn_str = "tcp://"+ip+":5556"
  print "send a request:" , typ +' '+msg , "To:",conn_str
  req_socket.connect(conn_str)
  req_socket.setsockopt(zmq.RCVTIMEO, wtime)
  req_socket.setsockopt(zmq.LINGER, 0)
  req_socket.send(typ +' '+ msg) 
  mssg = ""
  try:
    mssg = req_socket.recv()
    print "rcv msg:", mssg
  except zmq.error.Again:
    print "No reply from ",conn_str
  req_socket.close()
  return mssg

def find_successor(idk):
  idk = int(idk)
  print "looking for succ of ",idk
  #self check
  if fc.in_lorc(idk, prcd.key, my.key, MOD): 
    print "I am succ of ",idk
    return my
  #succ
  if fc.in_lorc(idk, my.key, succ.key, MOD):
    print "My succ", succ.key, "is succ of",idk
    return succ
  else :
    np = closet_prcd_node(idk)
    print "route to other...", np.key
    msg = request(np.ip, 'FTS', str(idk))
    if not msg: #No answer, np failed
      print "Failed to find successor of ",idk
      print np.key, np.ip ,"Lost Conn "
      return Node()
    k, p = msg.split(' ')
    print "succ is ", k 
    return Node(int(k) , p)

def closet_prcd_node(idk):
  for i in range(M-1, -1,-1):
    if finger[i].key == -1:
      continue
    if fc.in_lorc(finger[i].key, my.key, idk, MOD):
      return finger[i]
  return my 

Socks = {} #k: pub_ip, v: SUB socket of ip
Onr   = {} #k: pub_ip, v: (s_key, t_key)
Con   = {} #k: pub_ip, v: last_time_stamp
Hst   = {} #k: evtkey, v: [h1,h2,h3,h4,h5]
#init history table
hnum = 5
for i in range (1, MOD+1):
  Hst[i] = Queue.Queue(hnum) 
  for j in range (hnum):
    Hst[i].put("None")

def registered(ip):
  return ip in Onr

def reg(k):
  Onr[k] = (0,1)
  update_onr_socks()

def un_reg(k):
  Onr.pop(k, None)
  update_onr_socks()

def update_onr_socks():
  update_onr()
  update_socks()

def update_onr():
  size = len(Onr)
  if size > 0:
    MIN = 1
    MAX = MOD
    totl = MAX - MIN 
    intrv = totl / size 
    i = 0
    for key in Onr:
      left  = MIN + intrv*i
      right = MIN + intrv*(i+1)
      if i == size - 1: #last interv, include rest
        right = MAX
      Onr[key] = (mmod(left), mmod(right))
      i += 1
    print "Updated Onr"
    print Onr

def update_socks():
  for ip in Socks:
    poller.unregister(Socks[ip])
    Socks[ip].close()
  Socks.clear()
  for ip in Onr:
    sock = context.socket(zmq.SUB)
    connect_str = "tcp://"+ip+":5555"
    sock.connect(connect_str)
    for i in range(Onr[ip][0], Onr[ip][1]+1):
      key_filter = str(i).decode('ascii')
      sock.setsockopt_string(zmq.SUBSCRIBE, key_filter)
    Socks[ip] = sock
    poller.register(sock, zmq.POLLIN)

#def pub_own_topic(pubk, topic):
#  top = int(topic)
#  #if pubk not in Onr :
#  #  un_reg(pubk)
#  #  return False
#  return fc.in_lorc(top, Onr[pubk][0], Onr[pubk][1], MOD)

def I_own_topic(topic):
  top = int(topic)
  return fc.in_lorc(top, prcd.key, my.key, MOD)

def update_Hst(key, smsg):
  Hst[key].get()
  Hst[key].put(smsg)

def get_Hst(key, num):
  his = ""
  num = min(hnum, num)
  for i in range(num) :
    ll = list(Hst[int(key)].queue)
    his = his + ll[num-1-i] + ' \n'  
  return his

def update_Conn(pubk, tsp):
  Con[pubk] = tsp

def check_pub_conn():
  cur = time.time()
  for k in Con:
    if cur - Con[k] > 3 : #hvn't pub in 3 secs, assume died 
      Con.pop(k, None)
      un_reg(k)
      print "Un registered", k 
      print "Current Con table"
      print Con 
      print "Current ownership"
      print Onr
      break
  return cur

def check_succ():
  newnd = Node(succ.key, succ.ip)
  msg = request(succ.ip, 'SCT', str(my.key)+' '+my.ip)
  print "send SCT request "
  Lost = False
  if not msg:
    Lost = True
    nxtsuc = suclst[1] #get next one in queue 
    msg = request(nxtsuc.ip, 'SCT', str(my.key)+' '+my.ip)
    print "send to my next succ",nxtsuc.ip
    newnd.key = nxtsuc.key
    newnd.ip  = nxtsuc.ip

  print "rcvd succ's suc list:" 
  print msg

  ll = msg.split('\n') 
  ll0 = ll[0].split()
  ll1 = ll[1].split()
  node1 = Node(int(ll0[0]), ll0[1])
  node2 = Node(int(ll1[0]), ll1[1])
  suclst[0] = newnd 
  suclst[1] = node1
  suclst[2] = node2
  if Lost:
    print "updated new suc list:"
    for i in range(len(suclst)):
      print suclst[i].key,suclst[i].ip 
  return newnd

def fix_finger(nxt):
  nxt = (nxt + 1) % M
  print "Fix finger"
  print "fixing:", mmod(my.key+2**nxt)
  nod = find_successor(mmod(my.key+2**nxt))
  print "succ is:",nod.key
  finger[nxt] = nod
  print "CUR FINGER TB:"
  for i in range(len(finger)):
    print finger[i].key,finger[i].ip
  return nxt

init_scl()  
init_fgr()  
nxt = 0

period = 3000 + my.key*238
wtime = 1000

last_check = time.time()
while True:
  rcvsocks = dict(poller.poll(wtime))
  if reg_sock in rcvsocks:
    
    strr = reg_sock.recv()
    reply = "emm.."
    typ,msg = strr.split(' ',1)

    print "recv ", strr

    if typ == 'FTS' : #find successor with key: msg
      su = find_successor(int(msg))
      reply = str(su.key) + ' ' + su.ip
      print "reply ", reply

    elif typ == 'REG' and not registered(msg) : #puber regist to this node
      reply = "WELCOME NEW PUBER!" 
      reg(msg)   
      request(succ.ip, 'REG',msg) #notice to succ

    elif typ == 'HST' : #suber asking for history
      evtk, num = msg.split()
      reply = get_Hst(int(evtk), int(num))

    elif typ == 'SCT' : #prcd check for succ
      pk, pip= msg.split()
      pk = int(pk)
      if pk != prcd.key:
        prcd.key = pk
        prcd.ip  = pip
      reply = ""
      for i in range(len(suclst)):
        reply = reply + str(suclst[i].key) + ' ' + suclst[i].ip + ' \n'  
    reg_sock.send(reply)

  for ip in Socks:
    if Socks[ip] in rcvsocks:
      msg = Socks[ip].recv() 
      evtk, pubk, rst = msg.split(' ', 2)
      update_Conn(pubk, float(rst.split()[1])) #record time
      update_Hst(int(evtk), rst)
      if I_own_topic(int(evtk)):
        smsg = rst + " " + pubk + " " + str(my.key)
        #send: evtk temprature time pub_ip srv_key
        socket.send_string(evtk +" " + smsg) 
        #print "PUBLISH:", evtk +" " + smsg

  #check connection with puber after some secs
  #also check succer, finger
  if time.time() - last_check > period/1000 : 
    print "period check"
    ar = randrange(3)
    if  ar == 0:
      check_pub_conn()
    elif ar == 1:
      succ = check_succ()
    elif ar == 2:
      nxt = fix_finger(nxt)

    last_check = time.time()
