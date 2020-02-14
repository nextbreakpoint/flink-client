Vagrant.configure(2) do |config|
  config.vm.define "flink-client" do |s|
    s.ssh.forward_agent = true
    s.vm.box = "ubuntu/bionic64"
    s.vm.hostname = "integration"
    s.vm.network "private_network",
      ip: "192.168.1.20",
      netmask: "255.255.255.0",
      auto_config: true
    s.vm.provision :shell,
      path: "pipeline/setup-docker.sh",
      privileged: false
    s.vm.provision :shell,
      path: "pipeline/setup-java.sh",
      privileged: false
    s.vm.provider "virtualbox" do |v|
      v.name = "integration"
      v.cpus = 1
      v.memory = 2048
      v.gui = false
      v.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
      #v.customize ["modifyvm", :id, "--natdnsproxy1", "on"]
    end
  end
end
