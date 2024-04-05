### REHYDRATION TRIGGER

resource "aws_lambda_function" "rehydration_fargate_trigger_lambda" {
  description                    = "Lambda Function which triggers FARGATE to start rehydration task"
  function_name                  = "${var.environment_name}-${var.service_name}-fargate-trigger-lambda-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  reserved_concurrent_executions = 1 // don't allow concurrent lambda's
  handler                        = "bootstrap"
  runtime                        = "provided.al2"
  architectures                  = ["arm64"]
  role                           = aws_iam_role.rehydration_lambda_role.arn
  timeout                        = 30
  memory_size                    = 128
  s3_bucket                      = var.lambda_bucket
  s3_key                         = "${var.service_name}/service/rehydration-service-${var.image_tag}.zip"

  vpc_config {
    subnet_ids         = tolist(data.terraform_remote_state.vpc.outputs.private_subnet_ids)
    security_group_ids = [
      data.terraform_remote_state.platform_infrastructure.outputs.rehydration_service_security_group_id
    ]
  }

  environment {
    variables = {
      ENV                                    = var.environment_name
      TASK_DEF_ARN                           = aws_ecs_task_definition.rehydration_ecs_task_definition.arn,
      CLUSTER_ARN                            = data.terraform_remote_state.fargate.outputs.ecs_cluster_arn,
      SUBNET_IDS                             = join(",", data.terraform_remote_state.vpc.outputs.private_subnet_ids),
      SECURITY_GROUP                         = data.terraform_remote_state.platform_infrastructure.outputs.rehydration_fargate_security_group_id,
      REGION                                 = var.aws_region,
      LOG_LEVEL                              = "info",
      TASK_DEF_CONTAINER_NAME                = var.tier,
      PENNSIEVE_DOMAIN                       = data.terraform_remote_state.account.outputs.domain_name
      FARGATE_IDEMPOTENT_DYNAMODB_TABLE_NAME = aws_dynamodb_table.idempotency_table.name,
      REQUEST_TRACKING_DYNAMODB_TABLE_NAME   = aws_dynamodb_table.tracking_table.name,
      REHYDRATION_TTL_DAYS                   = local.rehydration_ttl_days,
    }
  }
}