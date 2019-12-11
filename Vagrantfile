# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

  config.vm.box = "bento/ubuntu-18.04"

  config.vm.network "private_network", ip: "172.31.1.20"

  config.vm.network "forwarded_port", guest: 9092, host: 9092  # Kafka
  config.vm.network "forwarded_port", guest: 8088, host: 8088  # KSQL
  config.vm.network "forwarded_port", guest: 8083, host: 8083  # Kafka Connect
  config.vm.network "forwarded_port", guest: 8082, host: 8082  # Kafka REST Proxy
  config.vm.network "forwarded_port", guest: 8081, host: 8081  # Schema Registry

  config.ssh.forward_agent = true

  config.vm.synced_folder ".", "/vagrant", type: "nfs"

  config.vm.provider "vmware_fusion" do |vf|
    vf.vmx["memsize"] = "4096"
  end

  config.vm.provider "virtualbox" do |vb, override|
    vb.memory = 4096
  end
end
