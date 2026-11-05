# Continuous deployment: the IAM user GitHub Actions uses to push the images and
# redeploy the stream instance. Its access key is stored as an encrypted GitHub secret.
#
# Keyless OIDC would be better, but this account's organisation policy blocks creating
# an OIDC provider, so the key is limited to exactly what deploying needs instead.

resource "aws_iam_user" "github_deploy" {
  name = "tfl-github-deploy"
}

data "aws_iam_policy_document" "github_deploy" {
  statement {
    sid       = "EcrLogin"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"] # this action can't be limited to a repository
  }

  statement {
    sid = "PushImages"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:InitiateLayerUpload",
      "ecr:UploadLayerPart",
      "ecr:CompleteLayerUpload",
      "ecr:PutImage",
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
    ]
    resources = [for repo in aws_ecr_repository.images : repo.arn]
  }

  statement {
    sid     = "RedeployStream"
    actions = ["ssm:SendCommand"]
    resources = [
      aws_instance.stream.arn,
      "arn:aws:ssm:${data.aws_region.current.region}::document/AWS-RunShellScript",
    ]
  }

  statement {
    sid       = "FindStreamInstance"
    actions   = ["ec2:DescribeInstances"]
    resources = ["*"] # read-only, and Describe actions can't be limited to an instance
  }

  statement {
    sid       = "ReadCommandResult"
    actions   = ["ssm:GetCommandInvocation"]
    resources = ["*"] # this action can't be limited to an instance
  }
}

resource "aws_iam_user_policy" "github_deploy" {
  name   = "push-images-and-redeploy"
  user   = aws_iam_user.github_deploy.name
  policy = data.aws_iam_policy_document.github_deploy.json
}