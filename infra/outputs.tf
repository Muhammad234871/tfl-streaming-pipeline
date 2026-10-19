output "lake_bucket_name" {
  description = "S3 bucket for the data lake; use it as LAKE_URI=s3://<name>"
  value       = aws_s3_bucket.lake.bucket
}