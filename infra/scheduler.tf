# Starts the batch instance every night at 01:00 UTC, an hour after the day ends, so the
# consumer has written yesterday's last files. The instance shuts itself down when done.

data "aws_iam_policy_document" "scheduler_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "tfl-batch-scheduler"
  description        = "Lets EventBridge Scheduler start the batch instance, nothing else"
  assume_role_policy = data.aws_iam_policy_document.scheduler_trust.json
}

data "aws_iam_policy_document" "start_batch" {
  statement {
    actions   = ["ec2:StartInstances"]
    resources = [aws_instance.batch.arn]
  }
}

resource "aws_iam_role_policy" "start_batch" {
  name   = "start-batch-instance"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.start_batch.json
}

resource "aws_scheduler_schedule" "batch_nightly" {
  name                         = "tfl-batch-nightly"
  description                  = "Start the batch instance; it processes yesterday and stops itself"
  schedule_expression          = "cron(0 1 * * ? *)"
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode = "OFF"
  }

  # A "universal target": the scheduler calls the EC2 API directly, no Lambda needed.
  target {
    arn      = "arn:aws:scheduler:::aws-sdk:ec2:startInstances"
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ InstanceIds = [aws_instance.batch.id] })
  }
}