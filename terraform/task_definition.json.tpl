[
  {
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
        "awslogs-group":"/aws/fargate/${environment_name}-${service_name}-${tier}-${aws_region_shortname}",
        "awslogs-region": "${aws_region}",
        "awslogs-stream-prefix": "fargate"
      }
    },
    "environment": [
      { "name" : "ENVIRONMENT", "value": "${environment_name}" },
      { "name" : "ENV", "value": "${environment_name}" },
      { "name" : "REGION", "value": "${aws_region}" },
      { "name" : "REHYDRATION_BUCKET", "value": "${rehydration_bucket}" },
      { "name" : "REHYDRATION_TTL_DAYS", "value": "${rehydration_ttl_days}" }
    ],
    "name": "${tier}",
    "image": "${image_url}:${image_tag}",
    "cpu": ${container_cpu},
    "memory": ${container_memory},
    "essential": true,
    "repositoryCredentials": {
      "credentialsParameter": "${docker_hub_credentials}"
    }
  }
]
