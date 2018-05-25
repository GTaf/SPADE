#!/bin/bash

curl -s https://packagecloud.io/install/repositories/camflow/provenance/script.rpm.sh | sudo bash
sudo dnf install camflow
sudo systemctl enable camconfd.service
sudo systemctl enable camflowd.service

echo "you should now reboot your computer to start with CamFlow kernel"
