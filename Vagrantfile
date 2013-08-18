# -*- mode: ruby -*-
# vi: set ft=ruby :

# Use vagrant version 2
Vagrant.configure("2") do |config|

  # All machines will use a common CentOS-6 (64 bit) base
  config.vm.box     = "centos-64-x64-vbox4210"
  config.vm.box_url = "http://puppet-vagrant-boxes.puppetlabs.com/centos-64-x64-vbox4210.box"

  config.vm.synced_folder "salt/roots/", "/srv/"

  config.vm.network :private_network, ip: "192.168.100.100"
  config.vm.hostname = "visualiser"

  config.vm.provision :salt do |salt|
    salt.minion_config = "salt/minion"
    salt.run_highstate = true
    salt.verbose = true
  end

end
