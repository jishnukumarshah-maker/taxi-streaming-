terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "checkpoint_bucket" {
  bucket_prefix = "nyc-taxi-streaming-checkpoints-"
  force_destroy = true
}

resource "aws_budgets_budget" "cost_alert" {
  name              = "monthly-budget-cap"
  budget_type       = "COST"
  limit_amount      = "15.0"
  limit_unit        = "USD"
  time_unit         = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = ["alert@example.com"]
  }
}

output "checkpoint_bucket_name" {
  value = aws_s3_bucket.checkpoint_bucket.id
}
