# One role per EC2 instance. The code on an instance gets the role's temporary
# credentials automatically (no access keys anywhere).
locals {
  instances = {
    stream = {
      pipeline_policy_arn = aws_iam_policy.consumer_write_bronze.arn
      image               = "tfl-stream"
    }
    batch = {
      pipeline_policy_arn = aws_iam_policy.spark_jobs.arn
      image               = "tfl-batch"
    }
  }
}

# Who may use the role: the EC2 service, on behalf of an instance.
data "aws_iam_policy_document" "ec2_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "instance" {
  for_each = local.instances

  name               = "tfl-${each.key}-instance"
  description        = "Role for the ${each.key} EC2 instance"
  assume_role_policy = data.aws_iam_policy_document.ec2_trust.json
}

# Pull only this instance's own image from ECR.
data "aws_iam_policy_document" "ecr_pull" {
  for_each = local.instances

  statement {
    sid       = "EcrLogin"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"] # this action can't be limited to a repository
  }

  statement {
    sid = "PullOwnImage"
    actions = [
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchCheckLayerAvailability",
    ]
    resources = [aws_ecr_repository.images[each.value.image].arn] # the ARN of this instance's repository:
  }
}

resource "aws_iam_role_policy" "ecr_pull" {
  for_each = local.instances

  name   = "ecr-pull-${each.value.image}"
  role   = aws_iam_role.instance[each.key].id
  policy = data.aws_iam_policy_document.ecr_pull[each.key].json
}

resource "aws_iam_role_policy_attachment" "pipeline" {
  for_each = local.instances

  role       = aws_iam_role.instance[each.key].name
  policy_arn = each.value.pipeline_policy_arn
}

resource "aws_iam_role_policy_attachment" "ssm" {
  for_each = local.instances

  role       = aws_iam_role.instance[each.key].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# An EC2 instance can't use a role directly: it uses an instance profile that holds it.
resource "aws_iam_instance_profile" "instance" {
  for_each = local.instances

  name = aws_iam_role.instance[each.key].name # the same name as the role
  role = aws_iam_role.instance[each.key].name # the role's name
}