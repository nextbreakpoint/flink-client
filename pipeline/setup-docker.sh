#!/usr/bin/env sh

set -x
set -e

sudo apt-get update -y && sudo apt-get install -y apt-transport-https ca-certificates software-properties-common curl jq socat
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable"
sudo apt-get update -y && sudo apt-get install -y docker-ce
sudo usermod -aG docker $USER

sudo docker network create flink-test
