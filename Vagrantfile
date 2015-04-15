Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.provider "virtualbox" do |vb|
    vb.memory="2048"
  end
  config.vm.provision :shell, inline: <<-SHELL
sudo apt-get update
sudo apt-get install -y emacs24-nox python-sqlalchemy
SHELL
end
