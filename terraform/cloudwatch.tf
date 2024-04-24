// CREATE FARGATE TASK CLOUDWATCH LOG GROUP
resource "aws_cloudwatch_log_group" "rehydration_fargate_cloudwatch_log_group" {
  name              = "/aws/fargate/${var.environment_name}-${var.service_name}-${var.tier}-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  retention_in_days = 14

  tags = local.common_tags
}

resource "aws_cloudwatch_log_subscription_filter" "rehydration_fargate_datadog_subscription" {
  name            = "${aws_cloudwatch_log_group.rehydration_fargate_cloudwatch_log_group.name}-subscription"
  log_group_name  = aws_cloudwatch_log_group.rehydration_fargate_cloudwatch_log_group.name
  filter_pattern  = ""
  destination_arn = data.terraform_remote_state.region.outputs.datadog_delivery_stream_arn
  role_arn        = data.terraform_remote_state.region.outputs.cw_logs_to_datadog_logs_firehose_role_arn
}

// CREATE SERVICE LAMBDA CLOUDWATCH LOG GROUP
resource "aws_cloudwatch_log_group" "rehydration_fargate_trigger_lambda_cloudwatch_log_group" {
  name              = "/aws/lambda/${aws_lambda_function.rehydration_fargate_trigger_lambda.function_name}"
  retention_in_days = 14

  tags = local.common_tags
}

resource "aws_cloudwatch_log_subscription_filter" "rehydration_fargate_trigger_lambda_datadog_subscription" {
  name            = "${aws_cloudwatch_log_group.rehydration_fargate_trigger_lambda_cloudwatch_log_group.name}-subscription"
  log_group_name  = aws_cloudwatch_log_group.rehydration_fargate_trigger_lambda_cloudwatch_log_group.name
  filter_pattern  = ""
  destination_arn = data.terraform_remote_state.region.outputs.datadog_delivery_stream_arn
  role_arn        = data.terraform_remote_state.region.outputs.cw_logs_to_datadog_logs_firehose_role_arn
}

// CREATE EXPIRATION LAMBDA CLOUDWATCH LOG GROUP
resource "aws_cloudwatch_log_group" "expiration_lambda_cloudwatch_log_group" {
  name              = "/aws/lambda/${aws_lambda_function.expiration_lambda.function_name}"
  retention_in_days = 14

  tags = local.common_tags
}

resource "aws_cloudwatch_log_subscription_filter" "expiration_lambda_datadog_subscription" {
  name            = "${aws_cloudwatch_log_group.expiration_lambda_cloudwatch_log_group.name}-subscription"
  log_group_name  = aws_cloudwatch_log_group.expiration_lambda_cloudwatch_log_group.name
  filter_pattern  = ""
  destination_arn = data.terraform_remote_state.region.outputs.datadog_delivery_stream_arn
  role_arn        = data.terraform_remote_state.region.outputs.cw_logs_to_datadog_logs_firehose_role_arn
}

// CREATE EXPIRATION EVENT RULE
resource "aws_cloudwatch_event_rule" "expiration_cloudwatch_event_rule" {
  name                = "${var.environment_name}-rehydration-expiration-cloudwatch-event-rule-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  description         = "Daily trigger for rehydration expiration"
  schedule_expression = "cron(0 4 * * ? *)"
}

resource "aws_cloudwatch_event_target" "expiration_cloudwatch_event_target" {
  rule      = aws_cloudwatch_event_rule.expiration_cloudwatch_event_rule.name
  target_id = "${var.environment_name}-rehydration-expiration-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  arn       = aws_lambda_function.expiration_lambda.arn
}