# Monitoring, step 1: container logs go to CloudWatch Logs, one log group per instance.
# The batch instance shuts down after each run, so this is the only way to read its
# logs afterwards. Old logs are deleted after 14 days.

resource "aws_cloudwatch_log_group" "instance" {
  for_each = local.instances

  name              = "/tfl/${each.key}"
  retention_in_days = 14
}

# Monitoring, step 2: count log lines that mean "it's working", as CloudWatch metrics.
# No code change: the consumer and run_day already print these lines.

# One per file the consumer writes to S3 (normally 2 every 5 minutes: arrivals + line status).
resource "aws_cloudwatch_log_metric_filter" "bronze_files_written" {
  name           = "tfl-bronze-files-written"
  log_group_name = aws_cloudwatch_log_group.instance["stream"].name
  pattern        = "\"consumer: wrote\""

  metric_transformation {
    namespace = "TfL/Pipeline"
    name      = "BronzeFilesWritten"
    value     = "1"
    default_value = "0" # in case the stream instance is down and no logs are coming in, so the metric is missing
  }
}

# One per successful nightly batch: run_day prints "day <dt> done: all N steps succeeded".
resource "aws_cloudwatch_log_metric_filter" "batch_succeeded" {
  name           = "tfl-batch-succeeded"
  log_group_name = aws_cloudwatch_log_group.instance["batch"].name
  pattern        = "\"steps succeeded\""

  metric_transformation {
    namespace = "TfL/Pipeline"
    name      = "BatchSucceeded"
    value     = "1"
    default_value = "0" # in case the batch instance is down and no logs are coming in, so the metric is missing
  }
}


# Monitoring, steps 3-5: alarms that e-mail through SNS.

resource "aws_sns_topic" "alerts" {
  name = "tfl-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email # confirm the subscription in the e-mail AWS sends
}

# No bronze file for 15 minutes: the stream has stopped (crash, Kafka, TfL API, or the
# instance is gone). Missing data counts as a problem, so "no logs at all" also alarms.
resource "aws_cloudwatch_metric_alarm" "no_bronze_data" {
  alarm_name          = "tfl-no-bronze-data"
  alarm_description   = "No bronze file written to S3 for 15 minutes"
  namespace           = "TfL/Pipeline"
  metric_name         = "BronzeFilesWritten"
  statistic           = "Sum"
  period              = 900
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

# No successful batch in the last 26 hours (it runs at 01:00 UTC and takes ~10 minutes,
# so 26 h leaves a margin). Hourly periods; it alarms only if all 26 had no success.
resource "aws_cloudwatch_metric_alarm" "batch_not_succeeded" {
  alarm_name          = "tfl-batch-not-succeeded"
  alarm_description   = "No successful nightly batch in the last 26 hours"
  namespace           = "TfL/Pipeline"
  metric_name         = "BatchSucceeded"
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 26
  datapoints_to_alarm = 26
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

# The hardware under the stream instance failed: move it to healthy hardware (same ID,
# disk and IP), and tell you about it.
resource "aws_cloudwatch_metric_alarm" "stream_recover" {
  alarm_name          = "tfl-stream-recover"
  alarm_description   = "Stream instance failed its system status check: recovering it"
  namespace           = "AWS/EC2"
  metric_name         = "StatusCheckFailed_System"
  dimensions          = { InstanceId = aws_instance.stream.id }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 2
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  alarm_actions = [
    "arn:aws:automate:${data.aws_region.current.region}:ec2:recover",
    aws_sns_topic.alerts.arn,
  ]
  ok_actions = [aws_sns_topic.alerts.arn]
}