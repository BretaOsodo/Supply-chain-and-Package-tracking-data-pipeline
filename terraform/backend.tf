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

  #backend "s3" {
  # bucket = "supply-chain-tf-state-23712434025
  # region = "eu-north-1"
  #dynamodb_table = "supply-chain-terraform-state-locking"
  #encrypt = true
  #}
}

