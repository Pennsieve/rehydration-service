## Service Lambda arn
output "rehydrate_service_arn" {
  value = aws_lambda_function.rehydrate_fargate_trigger_lambda.arn
}