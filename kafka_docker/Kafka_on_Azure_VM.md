* This experiment will first create an ubuntu VM on Azure and then install the required components (docker, docker-compose, and kafka installs).

<pre>
sudo apt update
sudo apt install apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable"
apt-cache policy docker-ce
sudo apt install docker-ce
sudo systemctl status docker
sudo usermod -aG docker ${USER}

## Once this is done, disconnect from SSH and re-login for the groups to take effect.
## Note docker is added to the group of current user.
venkyuser@venkykafkavm:~$ groups
venkyuser adm dialout cdrom floppy sudo audio dip video plugdev netdev lxd docker

## Check docker ps to make sure
venkyuser@venkykafkavm:~$ docker ps
CONTAINER ID   IMAGE     COMMAND   CREATED   STATUS    PORTS     NAMES

## install docker-compose
sudo apt install -y docker-compose

## Clone repo and start the kafka container.
venkyuser@venkykafkavm:~$ git clone https://github.com/SowmyaVenky/AzureSynapseExperiments.git

cd /home/venkyuser/AzureSynapseExperiments/kafka_docker
docker-compose up -d 
</pre>

* Make sure that the docker containers for KAFKA have started. 
<img src="../images/docker-compose-ubuntu.png" />

* Enable the ports required (22181, 29092) on the network security group of the VM. This will ensure that external apps can connect to the VM via both the public IP and Private IP. Ofcourse this will never be allowed in real world scenarios. 

<img src="../images/nsg_setup_vm.png" />

* The telnet features are not enabled in windows, and we need to go to control panel, windows features and enable telnet client. Once done, we can test telnet connectivity to kafka running inside the Azure VM.

<img src="../images/telnet_enable_windows.png" />

<img src="../images/telnet_connect_test.png" />


