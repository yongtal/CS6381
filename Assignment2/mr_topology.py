
#
# Vanderbilt University, Computer Science
# CS4287-5287: Principles of Cloud Computing
# Author: Aniruddha Gokhale
# Created: Nov 2016
# 
#  Purpose: To define a topology class for our map reduce framework to run on
#

from random import randrange
from mininet.topo import Topo
from mininet.net import Mininet

# @NOTE@:  I do not think any change is needed to this logic

class MR_Topo (Topo):
    "Map Reduce Topology."
    # override the build method. We define the number of racks. If Racks == 1,
    # All the map and reduce nodes are on the same rack. If Racks==2, then master
    # node is on rack while map nodes are on second rack but reduce are back on
    # same switch as master node (sounds silly). If Racks==3 then the master is on one
    # rack, the map nodes on 2nd rack and reduce nodes on the third rack. Number of
    # switches equals the number of racks.
    
    def build (self, Racks=1, S=3, M=10, R=3):
        print "Topology: Racks = ", Racks, ", M = ", M, ", R = ", R
        self.mr_switches = []
        self.mr_hosts = []
        # Python's range(N) generates 0..N-1
        for r in range (Racks):
            # a switch per rack.
            switch = self.addSwitch ('s{}'.format(r+1))
            print "Added switch", switch
            self.mr_switches.append (switch)
            if (r > 0):
                # connect the switches
                self.addLink (self.mr_switches[r-1], self.mr_switches[r])
                print "Added link between", self.mr_switches[r-1], " and ", self.mr_switches[r]

        host_index = 0
        switch_index = 0

        #add S server to random swich 
        for h in range (S):
            switch_index = randrange(Racks)
            host = self.addHost('h{}s{}'.format (host_index+1, switch_index+1))
            print "Added next map host", host
            self.addLink(host, self.mr_switches[switch_index]) # zero based indexing
            print "Added link between ", host, " and switch ", self.mr_switches[switch_index]
            self.mr_hosts.append (host)
            host_index = host_index +1 

        #add M publisher to random swich 
        for h in range (M):
            switch_index = randrange(Racks)
            host = self.addHost('h{}s{}'.format (host_index+1, switch_index+1))
            print "Added next map host", host
            self.addLink(host, self.mr_switches[switch_index])
            print "Added link between ", host, " and switch ", self.mr_switches[switch_index]
            self.mr_hosts.append (host)
            host_index = host_index + 1 

        # Now add the R reduce nodes to the next available rack
        #$switch_index = (switch_index + 1) % Racks

        #add R subers to random swich 
        for h in range (R):
            switch_index = randrange(Racks)
            host = self.addHost('h{}s{}'.format (host_index+1, switch_index+1))
            print "Added next reduce host", host
            self.addLink(host, self.mr_switches[switch_index])
            print "Added link between ", host, " and switch ", self.mr_switches[switch_index]
            self.mr_hosts.append (host)
            host_index = host_index +1 


