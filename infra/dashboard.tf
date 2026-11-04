data "aws_region" "current" {}

locals {
  glue_arn_prefix = "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}"
}

data "aws_iam_policy_document" "dashboard" {
  statement {
    sid = "QueryInTflWorkgroup"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:StopQueryExecution",
      "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.tfl.arn]
  }

  statement {
    sid     = "ReadGoldTableDefinitions"
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
    resources = [
      "${local.glue_arn_prefix}:catalog",
      "${local.glue_arn_prefix}:database/${aws_glue_catalog_database.lake.name}",
      "${local.glue_arn_prefix}:table/${aws_glue_catalog_database.lake.name}/gold_*"
    ]
  }

  statement {
    sid       = "BucketLevel"
    actions   = ["s3:GetBucketLocation", "s3:ListBucketMultipartUploads"]
    resources = [aws_s3_bucket.lake.arn]
  }

  statement {
    sid       = "ListGoldAndResults"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["gold/*", "athena-results/*", "gold", "athena-results"]
    }
  }

  statement {
    sid       = "ReadGold"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/gold/*"]
  }

  statement {
    sid = "ReadWriteQueryResults"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.lake.arn}/athena-results/*"]
  }
}
