Run the program:

1. sh xterm.sh     //Open Mininet, Generate cmd.txt, there are 7 hosts
2. soucre cmd.txt  //Open 7 xterms, one for each host.
3. for host 1-4, input:
   python server.py node_key ip
   eg. for host1: python server.py 2 10.0.0.1
           host2: python server.py 4 10.0.0.2
           host3: python server.py 6 10.0.0.3
           host4: python server.py 8 10.0.0.4

4. for host 5-6, input:
   python publisher.py ip

5. for host 7, input:
   python subscriber.py key
   Key is the topic, it can be any random number from 1 to 8

You can play with it by closing one server (anyone from host 2,3,4. but NOT host1, because host1 is used for registration), closing one publisher, and see the result.

Please see the report for details.
