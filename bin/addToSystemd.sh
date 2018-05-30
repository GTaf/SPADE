#!/bin/bash

sudo cp SPADE.service /etc/systemd/system
sudo chmod u+x spade
sudo sed -i 's/SELINUX=.*/SELINUX=disabled/g' /etc/sysconfig/selinux
systemctl daemon-reload
sudo systemctl enable SPADE.service
