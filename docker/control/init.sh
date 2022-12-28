#!/bin/sh

mkdir -m 700 ~/.ssh
ssh-keygen -t rsa -b 4096 -N '' -f ~/.ssh/id_rsa
cp ~/.ssh/id_rsa.pub /keys
touch /keys/control_ready

echo > ~/.ssh/known_hosts
for i in $(seq 1 5)
do
  while [ ! -f /keys/n${i}_ready ]
  do
    sleep 1
  done
  ssh-keyscan -t rsa n${i} >> ~/.ssh/known_hosts
done

rm /keys/*

# keep running
tail -f /dev/null
