#!/bin/bash

sudo apt update
sudo apt install python3-full
python3 -m venv .
source bin/activate
python3 -m pip install -r requirements.txt