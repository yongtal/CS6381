from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
import logging
import time
import zmq
import sys

logging.basicConfig()

myip = "10.0.0."+sys.argv[1]
tpcrange = 5
#zk = KazooClient(hosts="10.0.0.1:2181,10.0.0.2:2181,10.0.0.1:2181")
zk = KazooClient(hosts=myip+':2181')
zk.start()     

zk.ensure_path("/topic") 
zk.ensure_path("/leader") 

#init nodes
for i in range (tpcrange):
  try:
    zk.create("/topic/"+str(i),value = str(0), acl=None, ephemeral=False, sequence=False, makepath=False) 
  except NodeExistsError:
    pass
  try:
    zk.create("/topic/"+str(i)+"/power",value = str(0), acl=None, ephemeral=False, sequence=False, makepath=False) 
  except NodeExistsError:
    pass
  try:
    zk.create("/topic/"+str(i)+"/time",value = str(0), acl=None, ephemeral=False, sequence=False, makepath=False) 
  except NodeExistsError:
    pass 
  try:
    zk.create("/topic/"+str(i)+"/history",value = "0,0,0,0,0", acl=None, ephemeral=False, sequence=False, makepath=False) 
  except NodeExistsError:
    break

print "Create Nodes Finished!"

#leader election 
isLeader = False
out = zk.command(cmd='stat')
if "leader" in out:
  print "I am the leader!!"
  zk.set("/leader",value=myip)
  isLeader = True 
print "Leader Election Finished!"

#ZMQ part
context = zmq.Context()
reg_sock = context.socket(zmq.REP)
connect_str = "tcp://*:5556"
reg_sock.bind(connect_str)

#publish events from evt srv to subers
socket = context.socket(zmq.PUB)
socket.bind("tcp://*:5557")

def publish(tpc,val,stime,myip,pip,pwr):
  socket.send_string(tpc+" "+val+" "+stime + " "+myip+" "+pip+" "+pwr)
  print "publishing!"
  #myip is leader ip

print "Prepare ZMQ Finished! Listening..."
#check connection of each topic
def conn_update():
  print "Puber Conn Update"
  for tpc in range(tpcrange):
    data, stat = zk.get("/topic/"+str(tpc)+"/time")
    ltime = float(data)
    #print time.time(), ltime
    if time.time() - ltime > 10:
      zk.set("/topic/"+str(tpc)+"/power","0") 
  return time.time()

def leader_update():
  out = zk.command(cmd='stat')
  if "leader" in out:
    print "Leader Update: I am the leader!!"
    zk.set("/leader",value=myip)
    isLeader = True
  return time.time()


wtime = 10
last_check = time.time()
while True:

  strr = reg_sock.recv()
  print strr
  reply = 'Hey'
  
  typ,msg = strr.split(' ',1)

  if typ == 'leader':
    data, stat = zk.get("/leader")
    reply = leader_ip = data

  elif typ == 'HST':
    tpc,num = msg.split(' ',1)
    data, stat = zk.get("/topic/"+tpc+"/history")
    reply = data
    

  elif typ == 'pub':
    tpc, pwr, val, pip, stime = msg.split(' ',4)
    data, stat = zk.get("/topic/"+tpc+"/power")
    tpwr = int(data)
    npwr = int(pwr)
    if npwr < tpwr:
      pass

    #update this topic
    elif npwr == tpwr:
      zk.set("/topic/"+tpc,val)
      zk.set("/topic/"+tpc+"/time",stime)

      data,stat = zk.get("/topic/"+tpc+"/history")
      a,b,c,d,e = data.split(",",4)
      nh = b +"," + c + "," + d + "," + e + "," + val
      zk.set("/topic/"+tpc+"/history",nh)
      
      publish(tpc,val,stime,myip,pip,pwr)
      
    #new puber with higher power
    elif npwr > tpwr: 
      zk.set("/topic/"+tpc,val)
      zk.set("/topic/"+tpc+"/power",pwr)
      zk.set("/topic/"+tpc+"/time",stime)

      data,stat = zk.get("/topic/"+tpc+"/history")
      a,b,c,d,e = data.split(",",4)
      nh = b +"," + c + "," + d + "," + e + "," + val
      zk.set("/topic/"+tpc+"/history",nh)

      publish(tpc,val,stime,myip,pip,pwr)

  reg_sock.send(reply)  

  if time.time() - last_check > 10:
    last_check = conn_update()
    last_check = leader_update()
    
