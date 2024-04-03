resource "aws_dynamodb_table" "idempotency_table" {
  name         = "${var.environment_name}-rehydration-idempotency-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = merge(
    local.common_tags,
    {
      "Name"         = "${var.environment_name}-rehydration-idempotency-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
      "name"         = "${var.environment_name}-rehydration-idempotency-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
      "service_name" = var.service_name
    },
  )
}

resource "aws_dynamodb_table" "tracking_table" {
  name         = "${var.environment_name}-rehydration-tracking-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }

  attribute {
    name = "datasetVersion"
    type = "S"
  }

  attribute {
    name = "rehydrationStatus"
    type = "S"
  }

  attribute {
    name = "emailSentDate"
    type = "S"
  }

  global_secondary_index {
    name               = "DatasetVersionIndex"
    hash_key           = "datasetVersion"
    range_key          = "rehydrationStatus"
    projection_type    = "INCLUDE"
    non_key_attributes = ["id", "userName", "userEmail", "emailSentDate"]
  }

  global_secondary_index {
    name               = "ExpirationIndex"
    hash_key           = "rehydrationStatus"
    range_key          = "emailSentDate"
    projection_type    = "INCLUDE"
    non_key_attributes = ["datasetVersion"]
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = merge(
    local.common_tags,
    {
      "Name"         = "${var.environment_name}-rehydration-tracking-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
      "name"         = "${var.environment_name}-rehydration-tracking-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
      "service_name" = var.service_name
    },
  )
}
