#!/usr/bin/python

#
# Vanderbilt University, Computer Science
# CS4287-5287: Principles of Cloud Computing
# Author: Aniruddha Gokhale
# Created: Nov 2016
# 
#  Purpose: The code here is used to demonstrate the homegrown wordcount
# MapReduce framework on a network topology created using Mininet SDN emulator
#
# The mininet part is based on examples from the mininet distribution. The MapReduce
# part has been modified from the earlier thread-based implementation to a more
# process-based implementation required for this sample code
#

import os              # OS level utilities
import sys
import argparse   # for command line parsing

from signal import SIGINT
from time import time
from random import randrange

import subprocess

# These are all Mininet-specific
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.net import CLI
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel, info
from mininet.util import pmonitor

# This is our topology class created specially for Mininet
from mr_topology import MR_Topo

##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # @NOTE@: You might need to make appropriate changes
    #                          to this logic. Just make sure.

    # add optional arguments
    parser.add_argument ("-p", "--masterport", type=int, default=5557, help="Wordcount master port number, default 5557")
    parser.add_argument ("-r", "--racks", type=int, choices=[1, 2, 3], default=1, help="Number of racks, choices 1, 2 or 3")
    parser.add_argument ("-S", "--server", type=int, default=3, help="Number of server, default 3")
    parser.add_argument ("-M", "--maper", type=int, default=2, help="Number of Map jobs, default 10")
    parser.add_argument ("-R", "--reducer", type=int, default=3, help="Number of Reduce jobs, default 3")
    
    # add positional arguments in that order
    #parser.add_argument ("datafile", help="Big data file")

    # parse the args
    args = parser.parse_args ()

    return args
    
##################################
# Save the IP addresses of each host in our network
##################################
def saveIPAddresses (hosts, file="ipaddr.txt"):
    # for each host in the list, print its IP address in a file
    # The idea is that this file is now provided to the Wordcount
    # master program so it can use it to find the IP addresses of the
    # Map and Reduce worker machines
    try:
        file = open ("ipaddr.txt", "w")
        for h in hosts:
            file.write (h.IP () + "\n")

        file.close ()
        
    except:
            print "Unexpected error:", sys.exc_info()[0]
            raise


##################################
#  Generate the commands file to be sources
#
# @NOTE@: You will need to make appropriate changes
#                          to this logic.
##################################
def genCommandsFile (hosts, args):
    try:
        # first remove any existing out files
        for i in range (len (hosts)):
            # check if the output file exists
            if (os.path.isfile (hosts[i].name+".out")):
                os.remove (hosts[i].name+".out")

        # create the commands file. It will overwrite any previous file with the
        # same name.
        cmds = open ("cmd.txt", "w")

        # @NOTE@: You might need to make appropriate changes
        #                          to this logic by using the right file name and
        #                          arguments. My thinking is that the map and
        #                          reduce workers can be run as shown unless
        #                          you modify the command line params.

        # @NOTE@: for now I have commented the following line so we will have to
        # start the master manually on host h1s1

        # first create the command for the event server
        for i in range (args.server):
            #cmd_str = hosts[i].name + " python server.py " + str(i*2+2)+ " " + hosts[i].IP() + " " + str(3)  + " &> " + hosts[i].name + ".out &\n"
            cmd_str = "xterm " + hosts[i].name +"\n"
            cmds.write (cmd_str)

        #  next create the command for the map workers
        k = args.server
        for i in range (args.maper):
            cmd_str = hosts[i+k].name + " python publisher.py " + str(i*2+1)+ " " + hosts[i+k].IP() + " &> " + hosts[i+k].name + ".out &\n"
            cmds.write (cmd_str)

        #  next create the command for the reduce workers
        k = args.server + args.maper   # starting index for reducer hosts (master + maps)
        for i in range (args.reducer):
            cmd_str = hosts[k+i].name + " python subscriber.py " + str(i) + " " + hosts[i+k].IP() +  " &> " + hosts[k+i].name + ".out &\n"
            # make the last suber start subscribe after 10 seconds
            #if i == args.reduce -1 :
            #    cmd_str = hosts[k+i].name + " python subscriber.py " +  " " + hosts[i+k].IP() + " " + str(randrange(10000, 10100)) + " 7 " + " &> " + hosts[k+i].name + ".out &\n"

            #else :
            #    cmd_str = hosts[k+i].name + " python subscriber.py " +  " " + hosts[i+k].IP() + " " + str(randrange(10000, 10100)) + " &> " + hosts[k+i].name + ".out &\n"
            cmds.write (cmd_str)

        # close the commands file.
        cmds.close ()
        
    except:
            print "Unexpected error in run mininet:", sys.exc_info()[0]
            raise

######################
# main program
######################
def main ():
    "Create and run the Wordcount mapreduce program in Mininet topology"

    # parse the command line
    parsed_args = parseCmdLineArgs ()
    
    # instantiate our topology
    print "Instantiate topology"
    topo = MR_Topo (Racks=parsed_args.racks, S = parsed_args.server, M = parsed_args.maper, R = parsed_args.reducer)

    # create the network
    print "Instantiate network"
    net = Mininet (topo)

    # activate the network
    print "Activate network"
    net.start ()

    # debugging purposes
    print "Dumping host connections"
    dumpNodeConnections (net.hosts)

    # debugging purposes
    #print "Testing network connectivity"
    #net.pingAll ()
    
    #print "Running wordcount apparatus"
    # Unfortunately, I cannot get this to work :-(
    #runMapReduceWordCount (net.hosts, parsed_args)

    print "Generating commands file to be sourced"
    genCommandsFile (net.hosts, parsed_args)

    # run the cli
    CLI (net)

    # @NOTE@
    # You should run the generated commands by going to the
    # Mininet prompt on the CLI and typing:
    #     source commands.txt
    # Then, keep checking if all python jobs (except one) are completed
    # You can look at the *.out files which have all the debugging data
    # If there are errors in running the python code, these will also
    # show up in the *.out files.
    
    # cleanup
    net.stop ()

if __name__ == '__main__':
    # Tell mininet to print useful information
    setLogLevel('info')
    main ()
