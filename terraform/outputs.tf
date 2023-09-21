## Service Lambda arn
output "rehydration_service_arn" {
  value = aws_lambda_function.rehydrate_fargate_trigger_lambda.arn
}