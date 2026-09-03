provider "aws" {
  region = "eu-north-1"
}

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.17.0"
    }
  }
}

#S3 bucket

resource "aws_s3_bucket" "validated_data" {
  bucket = "supply-chain-and-package-tracking-validated"
  force_destroy = true
}

resource "aws_s3_bucket" "malformed_data" {
  bucket = "supply-chain-and-package-tracking-malformed-data"
}

#AMI
data "aws_ami" "debian" {
  most_recent = true
  owners = ["237124340255"]

  filter {
    name = "name"
    values = ["debian-12-amd64-*"]
  }

  filter {
    name = "virtualization-type"
    values = ["hvm"]
  }
}

#IAM ROLE (EC2 AND S3 ACCESS)
resource "aws_iam_role" "ec2" {
  name = "ec2-s3-role"

  assume_role_policy = jsondecode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_group_policy" "s3_access" {
  role = aws_iam_role.ec2.id

  policy = jsondecode({
    Version = "2012-10-17"
    Statement=[{
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      Resource = [
        aws_s3_bucket.validated_data.arn,
        "${aws_s3_bucket.validated_data.arn}/*"]
    }]
  })
}

resource "aws_iam_instance_profile" "ec2" {
  name = "ec2-s3-profile"
  role = aws_iam_role.ec2.name
}

#EC2 INSTANCE
resource "aws_instance" "this" {
  aRmi = data.aws_ami.debian.id
  instance_type = "t3.large"
  iam_instance_profile = aws_iam_instance_profile.ec2.name

  user_data = << EOF
    #!/bin/bash
    set -eux

    #log everything so we can troubleshoot bootstrapping
    exec > > (tee /var/log/supply-chain-user-data.log | logger -t supply-chain-user-data -s 2>/dev/console) 2>&1

    echo "=== Starting Supply chain deployment"

    #1. Install system packages
    apt-get update -y
    apt-get install -y\
      git \
      docker.io \
      curl \
      ca-certificates

    #2. Start Docker
    systemctl enable docker
    systemctl start docker

    #3. Make sure Docker compose is available
    if ! docker compose version >/dev/null 2>&1; then
        mkdir -p /usr/local/lib/docker/cli-plugins

        curl -SL \
          https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
          -o /usr/local/lib/docker/cli-plugins/docker-compose

        chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
    fi

    docker --version
    docker compose version

    # 4. Create Docker network
    docker network create supply_chain || true

    #5. Clone the project
    mkdir -p /opt

    if [ ! -d "/opt/Supply-chain-and-Package-tracking-data-pipeline/.git" ]; then
      git clone \
        https://github.com/BretaOsodo/Supply-chain-and-Package-tracking-data-pipeline.git \
        /opt/Supply-chain-and-Package-tracking-data-pipeline
    fi

    cd /opt/Supply-chain-and-Package-tracking-data-pipeline

    #6. Start the entire pipeline

    docker compose up -d --build

    #7. Show running containers
    docker compose ps

    echo "=== Supply Chain deployment complete ==="
  EOF

  tags = {
        Name = "supply-chan-pipeline"
}
}
