#!/bin/bash

sudo cp SPADE.service /stc/systemd/system
sudo chmod u+x spade
systemctl daemon-reload
sudo systemctl enable SPADE.service
