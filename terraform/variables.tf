variable "admin_cidr" {
  description = "CIDR vlock allowed to reach SSH and the pipeline's admin UIs (GRAFANA, KAFDROP, SPARK UI, pgAdmin"
  type        = string
}

variable "key_name" {
  description = "Name of an existing EC@ key pair to allow SSH access"
  type        = string
}

variable "instance_type" {
  description = "EC2 instance type for the pipeline host"
  type        = string
  default     = "t3.large"
}

variable "root_volume_size_gb" {
  description = "Root EBS volume size in GN. The full docker- compose stack needs headroom for images and checkpoints"
  type        = number
  default     = 60
}
