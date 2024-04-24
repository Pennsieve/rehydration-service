variable "aws_account" {}

variable "aws_region" {}

variable "environment_name" {}

variable "service_name" {}

variable "vpc_name" {}

variable "image_tag" {
  default = "latest"
}

// Fargate Task
variable "container_memory" {
  default = "2048"
}

variable "container_cpu" {
  default = "0"
}

variable "image_url" {
  default = "pennsieve/rehydrate"
}

variable "task_memory" {
  default = "2048"
}

variable "task_cpu" {
  default = "512"
}

variable "tier" {
  default = "rehydration"
}

variable "lambda_bucket" {
  default = "pennsieve-cc-lambda-functions-use1"
}

locals {
  domain_name = data.terraform_remote_state.account.outputs.domain_name
  hosted_zone = data.terraform_remote_state.account.outputs.public_hosted_zone_id

  rehydration_bucket_name        = "pennsieve-${var.environment_name}-rehydration-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  rehydration_logs_target_prefix = "${var.environment_name}/rehydration/s3/"

  rehydration_ttl_days = 14

  common_tags = {
    aws_account      = var.aws_account
    aws_region       = data.aws_region.current_region.name
    environment_name = var.environment_name
  }
}