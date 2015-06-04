Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.provider "virtualbox" do |vb|
    vb.memory="2048"
  end
  config.vm.network :forwarded_port, host: 5433, guest: 5432
  config.vm.provision :shell, inline: <<-SHELL
sudo apt-get update
sudo apt-get install -y emacs24-nox python-sqlalchemy python-pandas python-jinja2
# postgres 9.3
sudo apt-get install -y postgresql-9.3 postgresql-contrib-9.3 python-psycopg2
sudo -u postgres createuser domdb
sudo -u postgres createdb -O domdb domdb
sudo -u postgres psql -c "ALTER USER domdb WITH ENCRYPTED PASSWORD 'domdb';"
sudo sed -i /etc/postgresql/9.3/main/postgresql.conf -e "s/^#listen_addresses.*/listen_addresses = '*'/"
sudo echo "host domdb domdb 10.0.2.2/16 md5" >> /etc/postgresql/9.3/main/pg_hba.conf
# restart postgres
sudo service postgresql restart
SHELL
end
