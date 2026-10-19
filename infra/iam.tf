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