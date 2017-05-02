Usage:

  sh run.sh
   
  mininet> source cmd.txt


Adjust parameter:
  open run.sh, modify:
                      -r:  Number of Racks
                      -M:  Number of Publishers
                      -R:  Number of Subscribers 

Notice:
  Publisher and subscriber are randomly distributed among the network.
  One of the Publisher will stop send event after 7 secs,
  event server will detect this abnormal and reassign the ownership.
  The last subscriber will wait 5 secs before start, and then it will 
  ask for the last 5 samples of some topic. 
