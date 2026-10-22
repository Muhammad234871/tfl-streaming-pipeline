output "lake_bucket_name" {
  description = "S3 bucket for the data lake; use it as LAKE_URI=s3://<name>"
  value       = aws_s3_bucket.lake.bucket
}

output "consumer_role_arn" {
  description = "ARN of the least-privilege role for the Kafka consumer; use it as AWS_ROLE_ARN"
  value       = aws_iam_role.consumer.arn
}

output "spark_jobs_role_arn" {
  description = "IAM role the Spark jobs assume (read all layers, write silver and gold)"
  value       = aws_iam_role.spark_jobs.arn
}