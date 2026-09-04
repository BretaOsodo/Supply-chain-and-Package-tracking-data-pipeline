set -eux

#log everything so that we cab troubleshoot bootstrapping
exec > >(tee /var/log/supply-chain-user-data.log | logger -t supply-chain-user-data -s 2>/dev/console) 2>&1

echo "Starting Supply chain deployment"

#1. Install system packages
apt-get update -y
apt-get install -y \
  git \
  docker.io \
  curl \
  ca-certificates

#2. start `docker
systemctl enable docker
systemctl start docker

#3. Make sure docker compose is available
if ! docker compose version >/dev/null 2>&1; then
  mkdir -p /usr/local/lib/docker/cli-plugins

  curl -SL \
    https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
    -o /usr/local/lib/docker/cli-plugins/docker-compose

  chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
fi

docker --version
docker compose version

#4. create docker network
docker network create supply_chain || true

#5. clone the project
mkdir -p /opt

if [ ! -d "/opt/Supply-chain-and-Package-tracking-data-pipeline/.git" ]; then
  git clone \
    https://github.com/BretaOsodo/Supply-chain-and-Package-tracking-data-pipeline.git \
    /opt/Supply-chain-and-Package-tracking-data-pipeline
fi

cd /opt/Supply-chain-and-Package-tracking-data-pipeline

#6. Start the entire pipeline
docker compose up -d --build

#7. show running containers
docker compose ps

echo "Supply chain deployment complete"