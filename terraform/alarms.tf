# T1 CloudWatch alarms (EPIC 868m2zvjt; standard sets from
# pennsieve-infra-dashboard/docs/alarm-coverage-plan.md). The rehydration
# task itself is one-shot Fargate (no ALB) — its failures surface through
# the trigger lambda and tracking table. No alarm_actions yet.
module "service_alarms" {
  source = "git@github.com:Pennsieve/terraform-modules.git//service-alarms"

  environment_name = var.environment_name
  service_name     = var.service_name

  lambdas = {
    fargate-trigger = {
      function_name   = aws_lambda_function.rehydration_fargate_trigger_lambda.function_name
      timeout_seconds = aws_lambda_function.rehydration_fargate_trigger_lambda.timeout
    }
    expiration = {
      function_name   = aws_lambda_function.expiration_lambda.function_name
      timeout_seconds = aws_lambda_function.expiration_lambda.timeout
    }
  }

  dynamodb_tables = {
    idempotency = aws_dynamodb_table.idempotency_table.name
    tracking    = aws_dynamodb_table.tracking_table.name
  }
}
