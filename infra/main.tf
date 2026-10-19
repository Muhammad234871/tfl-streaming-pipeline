resource "aws_s3_bucket" "lake" {
  bucket_prefix = "tfl-lake-"
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket = aws_s3_bucket.lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-raw-data"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    expiration {
      days = 30
    }
  }
}