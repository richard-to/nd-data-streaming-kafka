# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

  config.vm.box = "bento/ubuntu-18.04"

  config.vm.network "private_network", ip: "172.31.1.20"

  config.ssh.forward_agent = true

  config.vm.synced_folder ".", "/vagrant", type: "nfs"

  config.vm.provider "vmware_fusion" do |vf|
    vf.vmx["memsize"] = "4096"
  end

  config.vm.provider "virtualbox" do |vb, override|
    vb.memory = 4096
  end
end
