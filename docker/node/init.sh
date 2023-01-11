#!/bin/sh

mkdir -p /var/run/sshd
sed -i "s/UsePrivilegeSeparation.*/UsePrivilegeSeparation no/g" /etc/ssh/sshd_config
sed -i "s/PermitRootLogin without-password/PermitRootLogin yes/g" /etc/ssh/sshd_config

# wait for updating the ssh key
while [ ! -f /keys/control_ready ]
do
  sleep 1
done

mkdir -p ~/.ssh
cat /keys/id_rsa.pub >> ~/.ssh/authorized_keys
hostname=$(hostname)
touch /keys/${hostname}_ready

exec /usr/sbin/sshd -D
