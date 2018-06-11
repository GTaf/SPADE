#!/bin/bash
SPADE_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/../ && pwd )"

sudo sed -i 's/ExecStart=.*/ExecStart=$SPADE_ROOT/bin/spade start/g' $SPADE_ROOT/bin/SPADE.service

sudo cp $SPADE_ROOT/bin/SPADE.service /etc/systemd/system
sudo chmod u+x spade
sudo sed -i 's/SELINUX=.*/SELINUX=disabled/g' /etc/sysconfig/selinux
systemctl daemon-reload
sudo systemctl enable SPADE.service
