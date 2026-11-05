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


output "athena_workgroup_name" {
  description = "Athena workgroup for queries on the data lake; use it as ATHENA_WORKGROUP"
  value       = aws_athena_workgroup.tfl.name
}


output "glue_catalog_database_name" {
  description = "Glue catalog database for the data lake; use it as GLUE_CATALOG_DATABASE"
  value       = aws_glue_catalog_database.lake.name
}


output "dashboard_user_name" {
  description = "IAM user for the deployed dashboard; create its access key with the CLI"
  value       = aws_iam_user.dashboard.name
}


output "ecr_repository_urls" {
  description = "Where to push the images"
  value       = { for name, repo in aws_ecr_repository.images : name => repo.repository_url }
}


output "stream_instance_id" {
  description = "Open a shell with: aws ssm start-session --target <id>"
  value       = aws_instance.stream.id
}


output "github_deploy_user_name" {
  description = "IAM user for GitHub Actions; create its access key with the CLI and store it as GitHub secrets"
  value       = aws_iam_user.github_deploy.name
}