# The batch instance: stopped most of the time. EventBridge Scheduler starts it every
# night; on boot it runs the daily batch for yesterday and then shuts itself down.

resource "aws_security_group" "batch" {
  name        = "tfl-batch"
  description = "Batch instance: no inbound, all outbound"
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "batch" {
  ami                    = data.aws_ssm_parameter.al2023.value
  instance_type          = "m7i-flex.large" # 2 vCPU, 8 GB: Spark needs the memory
  iam_instance_profile   = aws_iam_instance_profile.instance["batch"].name
  vpc_security_group_ids = [aws_security_group.batch.id]

  # "shutdown" inside the instance stops it (keeps the disk) instead of deleting it.
  instance_initiated_shutdown_behavior = "stop"

  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_size = 30 # the batch image is ~3 GB unpacked, plus Spark's temporary files
    volume_type = "gp3"
    encrypted   = true
  }

  user_data = templatefile("${path.module}/templates/batch_user_data.sh.tftpl", {
    region    = data.aws_region.current.region
    registry  = split("/", aws_ecr_repository.images["tfl-batch"].repository_url)[0]
    image     = "${aws_ecr_repository.images["tfl-batch"].repository_url}:latest"
    lake_uri  = "s3a://${aws_s3_bucket.lake.bucket}"
    log_group = aws_cloudwatch_log_group.instance["batch"].name
  })
  user_data_replace_on_change = true

  lifecycle {
    ignore_changes = [ami]
  }

  tags = {
    Name = "tfl-batch"
  }
}