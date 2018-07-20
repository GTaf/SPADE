#!/bin/bash

TESTUSER="local"
SPADE_HOME="/home/main/tmp2/SPADE"

sed -i -e '/format=/s/=.*/=spade_json/' /etc/camflowd.ini

_id=`id|cut -d"=" -f2 |cut -d"(" -f1`

if [ $_id != 0 ]; then 
  echo "This program must be run as root"
  exit
fi

_uid=`su - $TESTUSER -c "id" |cut -d"=" -f2 |cut -d"(" -f1`
echo $_uid
_tm=`date +%s`
if [ -e /var/log/audit/audit.log ]; then
   mv /var/log/audit/audit.log "/var/log/audit/audit.old."$_tm
fi

#starting camflow
#sudo camflow -a true
sudo camflow --track-file $1 propagate
sudo camflow --track-user local propagate
sudo camflow -e true

sleep 4 
echo "su - $TESTUSER -c $1"
su - $TESTUSER -c $1
sleep 4

sudo camflow -a false

sleep 10

sudo cp /tmp/audit.log $1".cflog"

#extract the EXECVE call

# create spade config and run spade


cd $SPADE_HOME

echo "add storage TextFile "$1".cf.txt" > cfg/spade.client.Control.config
echo "add storage Graphviz /tmp/provenance.dot" >> cfg/spade.client.Control.config
echo "add reporter CamFlow /tmp/audit.log" >> cfg/spade.client.Control.config


bin/spade start
sleep 5
bin/spade stop

sudo rm -rf /tmp/audit.log
sudo systemctl restart camflowd

#dot -Tpng $1".dot" > $1".png"




