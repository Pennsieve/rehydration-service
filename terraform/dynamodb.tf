resource "aws_dynamodb_table" "idempotency_table" {
  name         = "${var.environment_name}-rehydration-idempotency-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }

  attribute {
    name = "status"
    type = "S"
  }

  attribute {
    name = "expirationDate"
    type = "S"
  }

  global_secondary_index {
    name               = "ExpirationIndex"
    hash_key           = "status"
    range_key          = "expirationDate"
    projection_type    = "INCLUDE"
    non_key_attributes = ["id", "rehydrationLocation"]
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

  global_secondary_index {
    name               = "DatasetVersionIndex"
    hash_key           = "datasetVersion"
    range_key          = "rehydrationStatus"
    projection_type    = "INCLUDE"
    non_key_attributes = ["id", "userName", "userEmail", "emailSentDate"]
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
