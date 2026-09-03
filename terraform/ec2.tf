
data "aws_ami" "debian" {
  most_recent = true
  owners      = ["136693071363"]

  filter {
    name   = "name"
    values = ["debian-12-amd64-*"]
  }
  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
  filter {
    name   = "root-device-type"
    values = ["ebs"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

#EC2 Instance

resource "aws_instance" "this" {
  ami                    = data.aws_ami.debian.id
  instance_type          = var.instance_type
  key_name               = var.key_name
  iam_instance_profile   = aws_iam_instance_profile.ec2.name
  vpc_security_group_ids = [aws_security_group.pipeline.id]

  root_block_device {
    volume_size = var.root_volume_size_gb
    volume_type = "gp3"
  }

  user_data = file("${path.module}/user_data.sh")
  tags = {
    Name = "supply-chain-pipeline"
  }
}

#outputs
output "instance_id" {
  value = aws_instance.this.id
}

output "validated_data_bucket" {
  value = aws_s3_bucket.validated-data.bucket
}

output "malformed-data_bucket" {
  value = aws_s3_bucket.malformed_data.bucket
}