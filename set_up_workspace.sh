#!/bin/bash

# Install Docker (https://docs.docker.com/v17.09/engine/installation/linux/docker-ce/ubuntu/)

# Set up Repository
sudo apt-get update;

 sudo apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    software-properties-common;

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -;

sudo apt-key fingerprint 0EBFCD88;

sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable";


# Install Docker CE
sudo apt-get update;
sudo apt-get install docker-ce;



# Install Docker Compose (https://docs.docker.com/compose/install/)
sudo curl -L "https://github.com/docker/compose/releases/download/1.25.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose;
sudo chmod +x /usr/local/bin/docker-compose;
