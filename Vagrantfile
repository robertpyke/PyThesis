# -*- mode: ruby -*-
# vi: set ft=ruby :

# Use vagrant version 2
Vagrant.configure("2") do |config|

  # All machines will use a common CentOS-6 (64 bit) base
  config.vm.box     = "centos-64-x64-vbox4210"
  config.vm.box_url = "http://puppet-vagrant-boxes.puppetlabs.com/centos-64-x64-vbox4210.box"

  config.vm.synced_folder "salt/roots/", "/srv/"

  config.vm.network :private_network, ip: "192.168.100.100"
  config.vm.hostname = "thesis"

  config.vm.provision :salt do |salt|
    salt.minion_config = "salt/minion"
    salt.run_highstate = true
    salt.verbose = true

    salt.install_type = 'git'
    salt.install_args = 'v0.16.0'
  end

  # Overide default virtualbox config options
  config.vm.provider :virtualbox do |vb|
    # Give the VM 2GB of memory
    vb.customize ["modifyvm", :id, "--memory", "2048"]
  end

end
