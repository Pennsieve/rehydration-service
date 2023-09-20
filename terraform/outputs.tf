## Service Lambda arn
output "service_lambda_arn" {
  value = aws_lambda_function.fargate_trigger_lambda.arn
}