# REHYDRATION-FARGATE-TASK   #
##############################
resource "aws_iam_role" "rehydration_fargate_task_iam_role" {
  name = "${var.environment_name}-${var.service_name}-fargate-task-role-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  path = "/service-roles/"

  assume_role_policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
    {
        "Action": "sts:AssumeRole",
        "Effect": "Allow",
        "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
        }
    }
    ]
}
EOF

}

resource "aws_iam_role_policy_attachment" "rehydration_fargate_iam_role_policy_attachment" {
  role       = aws_iam_role.rehydration_fargate_task_iam_role.id
  policy_arn = aws_iam_policy.rehydration_iam_policy.arn
}

resource "aws_iam_policy" "rehydration_iam_policy" {
  name   = "${var.environment_name}-${var.service_name}-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  policy = data.aws_iam_policy_document.rehydration_fargate_iam_policy_document.json
}

data "aws_iam_policy_document" "rehydration_fargate_iam_policy_document" {
  statement {
    sid    = "TaskSecretsManagerPermissions"
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.docker_hub_credentials_arn,
      data.aws_kms_key.ssm_kms_key.arn,
    ]
  }

  statement {
    sid     = "TaskS3Permissions"
    effect  = "Allow"
    actions = [
      "s3:List*",
    ]
    resources = [
      "*",
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:*",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.discover_publish50_bucket_arn,
      "${data.terraform_remote_state.platform_infrastructure.outputs.discover_publish50_bucket_arn}/*",
    ]
  }

  statement {
    sid    = "RehydrationFargateDynamoDBPermissions"
    effect = "Allow"

    actions = [
      "dynamodb:UpdateItem",
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:DeleteItem",
      "dynamodb:Query",
    ]

    resources = [
      aws_dynamodb_table.idempotency_table.arn,
      "${aws_dynamodb_table.idempotency_table.arn}/*",
      aws_dynamodb_table.tracking_table.arn,
      "${aws_dynamodb_table.tracking_table.arn}/*",
    ]

  }

  statement {
    sid     = "RehydrationFargateSESPermissions"
    effect  = "Allow"
    actions = [
      "ses:SendEmail",
      "ses:SendRawEmail",
    ]
    resources = ["*"]
  }

  statement {
    sid     = "TaskLogPermissions"
    effect  = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutDestination",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams"
    ]
    resources = ["*"]
  }
}

# REHYDRATION-LAMBDA   #
##############################

resource "aws_iam_role" "rehydration_lambda_role" {
  name = "${var.environment_name}-${var.service_name}-rehydration_lambda_role-${data.terraform_remote_state.region.outputs.aws_region_shortname}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "rehydration_lambda_iam_policy_attachment" {
  role       = aws_iam_role.rehydration_lambda_role.name
  policy_arn = aws_iam_policy.rehydration_lambda_iam_policy.arn
}

resource "aws_iam_policy" "rehydration_lambda_iam_policy" {
  name   = "${var.environment_name}-${var.service_name}-rehydration-lambda-iam-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  path   = "/"
  policy = data.aws_iam_policy_document.rehydration_iam_policy_document.json
}

data "aws_iam_policy_document" "rehydration_iam_policy_document" {

  statement {
    sid    = "SecretsManagerPermissions"
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      data.terraform_remote_state.platform_infrastructure.outputs.docker_hub_credentials_arn,
      data.aws_kms_key.ssm_kms_key.arn,
    ]
  }

  statement {
    sid     = "RehydrationLambdaPermissions"
    effect  = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutDestination",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
      "ec2:CreateNetworkInterface",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DeleteNetworkInterface",
      "ec2:AssignPrivateIpAddresses",
      "ec2:UnassignPrivateIpAddresses"
    ]
    resources = ["*"]
  }

  statement {
    sid     = "ECSTaskPermissions"
    effect  = "Allow"
    actions = [
      "ecs:DescribeTasks",
      "ecs:RunTask",
      "ecs:ListTasks"
    ]
    resources = ["*"]
  }

  statement {
    sid     = "ECSPassRole"
    effect  = "Allow"
    actions = [
      "iam:PassRole",
    ]
    resources = [
      "*"
    ]
  }

  statement {
    sid    = "SSMPermissions"
    effect = "Allow"

    actions = [
      "ssm:GetParameter",
      "ssm:GetParameters",
      "ssm:GetParametersByPath",
    ]

    resources = [
      "arn:aws:ssm:${data.aws_region.current_region.name}:${data.aws_caller_identity.current.account_id}:parameter/${var.environment_name}/${var.service_name}/*"
    ]
  }

  statement {
    sid    = "RehydrationLambdaDynamoDBPermissions"
    effect = "Allow"

    actions = [
      "dynamodb:UpdateItem",
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:DeleteItem",
    ]

    resources = [
      aws_dynamodb_table.idempotency_table.arn,
      "${aws_dynamodb_table.idempotency_table.arn}/*",
      aws_dynamodb_table.tracking_table.arn,
      "${aws_dynamodb_table.tracking_table.arn}/*",
    ]

  }

  statement {
    sid     = "RehydrationLambdaSESPermissions"
    effect  = "Allow"
    actions = [
      "ses:SendEmail",
      "ses:SendRawEmail",
    ]
    resources = ["*"]
  }
}

