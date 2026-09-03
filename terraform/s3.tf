#s3 Buckets
resource "aws_s3_bucket" "validated-data" {
  bucket = "supply-chain-and-package-tracking-validated"
  force_destroy = true
}
resource "aws_s3_bucket_versioning" "bucket_versioning_validated_data" {
  bucket = aws_s3_bucket.validated_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bucket_crypto_conf_validated" {
  bucket = aws_s3_bucket.validated_data.bucket
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_account_public_access_block" "validated_data_block" {
  bucket = aws_s3_bucket.validated_data.id
  block_public_acls = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "malformed_data" {
  bucket        = "supply-chain-and-package-tracking-malformed-data"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "bucket_versioning_malformed_data" {
  bucket = aws_s3_bucket.malformed_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bucket_crypto_conf_malformed" {
  bucket = aws_s3_bucket.malformed_data.bucket
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_account_public_access_block" "malformed_data_block" {
  bucket = aws_s3_bucket.malformed_data.id
  block_public_acls = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket" "terraform_state" {
  bucket = "supply-chain-tf-state"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "terraform_bucket_versioning" {
  bucket = aws_s3_bucket.terraform_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state_crypto_conf" {
  bucket = aws_s3_bucket.terraform_state.bucket
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_account_public_access_block" "terraform_state_block" {
  bucket = aws_s3_bucket.terraform_state.id
  block_public_acls = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}