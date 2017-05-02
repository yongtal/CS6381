mkdir /home/lyt/zookeeper/z1
mkdir /home/lyt/zookeeper/z2
mkdir /home/lyt/zookeeper/z3

mkdir /home/lyt/zookeeper/logs_1
mkdir /home/lyt/zookeeper/logs_2
mkdir /home/lyt/zookeeper/logs_3

echo "1" > /home/lyt/zookeeper/z1/myid
echo "2" > /home/lyt/zookeeper/z2/myid
echo "3" > /home/lyt/zookeeper/z3/myid

../bin/zkServer.sh start zoo1.cfg

