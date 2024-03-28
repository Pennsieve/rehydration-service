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