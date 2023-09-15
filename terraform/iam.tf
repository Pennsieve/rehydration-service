// FARGATE TASK
# Create ECS Task IAM Role
resource "aws_iam_role" "fargate_task_iam_role" {
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

resource "aws_iam_role_policy_attachment" "fargate_iam_role_policy_attachment" {
  role       = aws_iam_role.fargate_task_iam_role.id
  policy_arn = aws_iam_policy.iam_policy.arn
}

resource "aws_iam_policy" "iam_policy" {
  name   = "${var.environment_name}-${var.service_name}-policy-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  policy = data.aws_iam_policy_document.rehydrate_fargate_iam_policy_document.json
}

data "aws_iam_policy_document" "rehydrate_fargate_iam_policy_document" {

  statement {
    effect = "Allow"

    actions = [
      "s3:List*",
    ]

    resources = [
      "*",
    ]
  }
}

