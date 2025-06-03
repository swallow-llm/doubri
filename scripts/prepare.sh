#!/bin/bash

# Set the timezone to Japan.
sudo timedatectl set-timezone Japan

# Install necessary packages.
sudo yum install lvm2 g++ cmake tbb tbb-devel parallel

# Prepare SSD drive and mount at /data .
sudo pvcreate /dev/nvme1n1 /dev/nvme2n1 /dev/nvme3n1 /dev/nvme4n1
sudo vgcreate ssdgroup /dev/nvme1n1 /dev/nvme2n1 /dev/nvme3n1 /dev/nvme4n1
sudo lvcreate -l 100%FREE -n lv0 ssdgroup
sudo mkfs -t ext4 /dev/ssdgroup/lv0
sudo mkdir /data
sudo mount /dev/ssdgroup/lv0 /data
sudo chmod 777 /data

# Mount S3 at /s3 . (don't forget to set an appropriate IAM role in advance)
wget https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.rpm
sudo yum install ./mount-s3.rpm
sudo mkdir /s3
sudo chmod 777 /s3
sudo chown ec2-user:ec2-user /s3
mount-s3 swallow-corpus-cc /s3

# Build Doubri binaries.
./scripts/build.sh
