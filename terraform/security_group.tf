
resource "aws_security_group" "pipeline" {
  name = "supply-chain-pipeline-sg"
  description = "SSH + admin UI access for the supply chain pipeline host"

  ingress {
    description = "SSH"
    from_port = 22
    to_port = 22
    protocol = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  ingress {
    description = "Spark Master Ui"
    from_port = 9090
    to_port = 9090
    protocol = "tcp"
    cidr_blocks = [var.admin_cidr]
  }
  ingress {
    description = "Spark Worker UIs"
    from_port = 9095
    to_port = 9101
    protocol = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  ingress {
    description = "Kafdrop UI"
    from_port = 9020
    to_port = 9020
    protocol = "tcp"
    cidr_blocks = [var.admin_cidr]

  }

  ingress {
    description = "Grafana"
    from_port = 3000
    to_port = 3000
    protocol = "tcp"
    cidr_blocks = [var.admin_cidr]

  }

  ingress {
    description = "Prometheus"
    from_port = 9096
    to_port = 9096
    protocol = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  ingress {
    description = "Kafka ecternal listeners"
    from_port = 9092
    to_port = 9093
    protocol = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  egress {
    description = "Allow all outbound"
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = {
    Name = "supply-chain-pipeline-sg"
  }
}