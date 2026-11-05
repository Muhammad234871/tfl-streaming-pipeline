# The latest Amazon Linux 2023 image, published by AWS in SSM Parameter Store.
data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

data "aws_vpc" "default" {
  default = true
}

# No inbound rules at all: nothing can connect in. Outbound is open, so the instance can
# reach the TfL API, S3, ECR and SSM (shell access goes through SSM, not SSH).
resource "aws_security_group" "stream" {
  name        = "tfl-stream"
  description = "Stream instance: no inbound, all outbound"
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "stream" {
  ami                    = data.aws_ssm_parameter.al2023.value
  instance_type          = "t3.small"
  iam_instance_profile   = aws_iam_instance_profile.instance["stream"].name
  vpc_security_group_ids = [aws_security_group.stream.id]

  # IMDSv2 only (blocks the old, easily abused metadata API). Hop limit 2 lets
  # containers reach the metadata service through Docker's network, so they get the
  # role's credentials; with the default of 1, boto3 in a container finds none.
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
    encrypted   = true
  }

  user_data = templatefile("${path.module}/templates/stream_user_data.sh.tftpl", {
    region    = data.aws_region.current.region
    registry  = split("/", aws_ecr_repository.images["tfl-stream"].repository_url)[0]
    image     = "${aws_ecr_repository.images["tfl-stream"].repository_url}:latest"
    lake_uri  = "s3://${aws_s3_bucket.lake.bucket}"
    log_group = aws_cloudwatch_log_group.instance["stream"].name
  })
  # A changed start-up script only runs on a new instance, so replace it on change.
  user_data_replace_on_change = true

  # A new Amazon Linux release shouldn't replace a running instance by itself.
  lifecycle {
    ignore_changes = [ami]
  }

  tags = {
    Name = "tfl-stream"
  }
}