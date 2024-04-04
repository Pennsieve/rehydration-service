## Rehydrate S3 Bucket ##
resource "aws_s3_bucket" "rehydration_s3_bucket" {
  bucket = local.rehydration_bucket_name

  lifecycle {
    prevent_destroy = true
  }

  tags = merge(
    local.common_tags,
    {
      "Name"         = "pennsieve-${var.environment_name}-rehydration-s3-bucket-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
      "name"         = "pennsieve-${var.environment_name}-rehydration-s3-bucket-${data.terraform_remote_state.region.outputs.aws_region_shortname}"
      "service_name" = "rehydration"
      "tier"         = "s3"
    },
  )
}

resource "aws_s3_bucket_cors_configuration" "rehydration_s3_bucket_cors" {
  bucket = aws_s3_bucket.rehydration_s3_bucket.bucket

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["*"]
    max_age_seconds = 3000
  }
}

resource "aws_s3_bucket_public_access_block" "rehydration_s3_bucket_public_access_block" {
  bucket = aws_s3_bucket.rehydration_s3_bucket.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

resource "aws_s3_bucket_policy" "rehydration_s3_bucket_policy" {
  bucket     = aws_s3_bucket.rehydration_s3_bucket.bucket
  policy     = data.aws_iam_policy_document.rehydration_bucket_iam_policy_document.json
  depends_on = [aws_s3_bucket_public_access_block.rehydration_s3_bucket_public_access_block]
}

resource "aws_s3_bucket_server_side_encryption_configuration" "rehydration_s3_bucket_encryption" {
  bucket = aws_s3_bucket.rehydration_s3_bucket.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_request_payment_configuration" "rehydration_s3_request_payer" {
  bucket = aws_s3_bucket.rehydration_s3_bucket.bucket
  payer  = "Requester"
}

resource "aws_s3_bucket_logging" "rehydration_s3_logging" {
  bucket = aws_s3_bucket.rehydration_s3_bucket.id

  target_bucket = data.terraform_remote_state.platform_infrastructure.outputs.discover_publish_logs_s3_bucket_id
  target_prefix = local.rehydration_logs_target_prefix
}