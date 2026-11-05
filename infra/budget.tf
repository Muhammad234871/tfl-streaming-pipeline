# Cost guardrail: the whole project may use at most $30 of AWS credits.
# Credits are excluded from the calculation, so the budget sees real usage
# (with credits included, it would always show $0).

variable "alert_email" {
  description = "Where budget alerts go. Set it in terraform.tfvars (not committed)."
  type        = string
}

locals {
  budget_limit_usd    = 30
  auto_stop_at_usd    = 25 # leaves a margin, as budget data lags by up to a day
  budget_period_start = "2026-11-01_00:00"
  instances_to_stop   = [aws_instance.stream.id]
}

resource "aws_budgets_budget" "project" {
  name              = "tfl-project-total"
  budget_type       = "COST"
  limit_amount      = tostring(local.budget_limit_usd)
  limit_unit        = "USD"
  time_unit         = "ANNUALLY" # one total for the project, not a monthly allowance
  time_period_start = local.budget_period_start

  cost_types {
    include_credit = false
    include_refund = false
  }

  # E-mails at 50 % and 80 % of actual spend, and when the forecast passes 100 %.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}

# The role AWS Budgets uses to stop the instances.
data "aws_iam_policy_document" "budgets_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["budgets.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "budget_action" {
  name               = "tfl-budget-action"
  description        = "Lets AWS Budgets stop the EC2 instances when the project budget is reached"
  assume_role_policy = data.aws_iam_policy_document.budgets_trust.json
}

resource "aws_iam_role_policy_attachment" "budget_action" {
  role       = aws_iam_role.budget_action.name
  policy_arn = "arn:aws:iam::aws:policy/AWSBudgetsActionsWithAWSResourceControlAccess"
}

# At $25 of real usage, stop the instances automatically (no approval needed).
resource "aws_budgets_budget_action" "stop_instances" {
  budget_name        = aws_budgets_budget.project.name
  action_type        = "RUN_SSM_DOCUMENTS"
  approval_model     = "AUTOMATIC"
  notification_type  = "ACTUAL"
  execution_role_arn = aws_iam_role.budget_action.arn

  action_threshold {
    action_threshold_type  = "ABSOLUTE_VALUE"
    action_threshold_value = local.auto_stop_at_usd
  }

  definition {
    ssm_action_definition {
      action_sub_type = "STOP_EC2_INSTANCES"
      region          = data.aws_region.current.region
      instance_ids    = local.instances_to_stop
    }
  }

  subscriber {
    address           = var.alert_email
    subscription_type = "EMAIL"
  }
}