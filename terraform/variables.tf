variable "aws_account" {}

variable "environment_name" {}

variable "service_name" {}

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

variable "image_tag" {
  default = "latest"
}

variable "tier" {
  default = "rehydrate"
}

locals {
  domain_name = data.terraform_remote_state.account.outputs.domain_name
  hosted_zone = data.terraform_remote_state.account.outputs.public_hosted_zone_id

  common_tags = {
    aws_account      = var.aws_account
    aws_region       = data.aws_region.current_region.name
    environment_name = var.environment_name
  }
}