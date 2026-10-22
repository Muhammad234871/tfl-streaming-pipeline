data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "consumer_write_bronze" {
  statement {
    effect = "Allow"
    actions = [
      "s3:PutObject"
    ]
    resources = [
      "${aws_s3_bucket.lake.arn}/bronze/*",
    ]
  }
}

resource "aws_iam_policy" "consumer_write_bronze" {
  name        = "tfl-consumer-write-bronze"
  description = "Allows the consumer to write objects under bronze/ in the lake bucket only"
  policy      = data.aws_iam_policy_document.consumer_write_bronze.json
}

data "aws_iam_policy_document" "consumer_trust" {
  statement {
    effect = "Allow"
    actions = [
      "sts:AssumeRole",
    ]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }
}

resource "aws_iam_role" "consumer" {
  name               = "tfl-consumer"
  description        = "Least-privilege role for the Kafka consumer"
  assume_role_policy = data.aws_iam_policy_document.consumer_trust.json
}

resource "aws_iam_role_policy_attachment" "consumer_write_bronze" {
  role       = aws_iam_role.consumer.name
  policy_arn = aws_iam_policy.consumer_write_bronze.arn
}

data "aws_iam_policy_document" "spark_jobs" {
  statement {
    sid       = "ListLakePrefixes"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["bronze/*", "silver/*", "gold/*", "bronze", "silver", "gold"]
    }
  }

  statement {
    sid     = "ReadAllLayers"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/bronze/*",
      "${aws_s3_bucket.lake.arn}/silver/*",
      "${aws_s3_bucket.lake.arn}/gold/*",
    ]
  }

  statement {
    sid     = "WriteSilverAndGold"
    actions = ["s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/silver/*",
      "${aws_s3_bucket.lake.arn}/gold/*",
    ]
  }
}

resource "aws_iam_policy" "spark_jobs" {
  name        = "tfl-spark-jobs"
  description = "Spark jobs: read bronze/silver/gold, write silver and gold only"
  policy      = data.aws_iam_policy_document.spark_jobs.json
}

resource "aws_iam_role" "spark_jobs" {
  name               = "tfl-spark-jobs"
  description        = "Least-privilege role for the Spark batch jobs"
  assume_role_policy = data.aws_iam_policy_document.consumer_trust.json
}

resource "aws_iam_role_policy_attachment" "spark_jobs" {
  role       = aws_iam_role.spark_jobs.name
  policy_arn = aws_iam_policy.spark_jobs.arn
}