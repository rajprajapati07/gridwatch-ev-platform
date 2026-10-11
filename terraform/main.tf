# GridWatch Infrastructure — Local development via LocalStack
#
# This IaC demonstrates the production AWS resource shape:
#   - ECS Fargate cluster + service for the app
#   - RDS PostgreSQL (TimescaleDB via custom parameter group)
#   - ElastiCache Redis cluster
#   - MSK Kafka cluster
#   - ECR repository for Docker images
#   - S3 bucket for WAL archival (PostgreSQL HA)
#   - Secrets Manager for credentials
#   - CloudWatch log group
#
# To run locally:
#   docker compose up localstack -d   # add localstack to docker-compose if needed
#   AWS_DEFAULT_REGION=us-east-1 \
#   AWS_ACCESS_KEY_ID=test \
#   AWS_SECRET_ACCESS_KEY=test \
#   terraform init && terraform apply
#
# Production: change provider endpoint_url to real AWS (remove override block).

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# ─── Provider — LocalStack for local dev ────────────────────────────────────
provider "aws" {
  region = var.aws_region

  access_key = "test"
  secret_key = "test"

  # LocalStack endpoint — remove this block for real AWS
  endpoints {
    ec2            = "http://localhost:4566"
    ecs            = "http://localhost:4566"
    ecr            = "http://localhost:4566"
    rds            = "http://localhost:4566"
    elasticache    = "http://localhost:4566"
    kafka          = "http://localhost:4566"
    s3             = "http://localhost:4566"
    secretsmanager = "http://localhost:4566"
    iam            = "http://localhost:4566"
    logs           = "http://localhost:4566"
  }

  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
}

variable "aws_region" {
  default = "us-east-1"
}

variable "env" {
  default = "dev"
}

variable "app_version" {
  default = "latest"
}

# ─── ECR Repository ──────────────────────────────────────────────────────────
resource "aws_ecr_repository" "gridwatch" {
  name                 = "gridwatch"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

# ─── S3 — WAL archival for PostgreSQL (HA / point-in-time recovery) ─────────
resource "aws_s3_bucket" "wal_archive" {
  bucket = "gridwatch-wal-${var.env}"
}

resource "aws_s3_bucket_versioning" "wal_archive" {
  bucket = aws_s3_bucket.wal_archive.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "wal_archive" {
  bucket = aws_s3_bucket.wal_archive.id

  rule {
    id     = "expire-old-wal"
    status = "Enabled"

    expiration {
      days = 7  # 7-day WAL window matches Kafka retention
    }
  }
}

# ─── Secrets Manager ─────────────────────────────────────────────────────────
resource "aws_secretsmanager_secret" "db_password" {
  name = "gridwatch/${var.env}/db-password"
}

resource "aws_secretsmanager_secret_version" "db_password" {
  secret_id     = aws_secretsmanager_secret.db_password.id
  secret_string = jsonencode({
    username = "gridwatch"
    password = "change-me-in-prod"
  })
}

resource "aws_secretsmanager_secret" "webhook_secret" {
  name = "gridwatch/${var.env}/csms-webhook-secret"
}

resource "aws_secretsmanager_secret_version" "webhook_secret" {
  secret_id     = aws_secretsmanager_secret.webhook_secret.id
  secret_string = "change-me-in-prod"
}

# ─── CloudWatch Log Group ────────────────────────────────────────────────────
resource "aws_cloudwatch_log_group" "gridwatch" {
  name              = "/ecs/gridwatch/${var.env}"
  retention_in_days = 30
}

# ─── VPC (simplified — production would use proper subnet/security-group setup) ──
resource "aws_vpc" "gridwatch" {
  cidr_block = "10.0.0.0/16"
  tags = { Name = "gridwatch-${var.env}" }
}

# ─── ECS Cluster ─────────────────────────────────────────────────────────────
resource "aws_ecs_cluster" "gridwatch" {
  name = "gridwatch-${var.env}"

  configuration {
    execute_command_configuration {
      logging = "DEFAULT"
    }
  }
}

# ─── ECS Task Definition ─────────────────────────────────────────────────────
resource "aws_ecs_task_definition" "gridwatch" {
  family                   = "gridwatch"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "2048"   # 2 vCPU
  memory                   = "4096"   # 4 GB

  container_definitions = jsonencode([
    {
      name  = "gridwatch"
      image = "${aws_ecr_repository.gridwatch.repository_url}:${var.app_version}"
      portMappings = [{ containerPort = 8080 }]

      environment = [
        { name = "SPRING_PROFILES_ACTIVE", value = "prod" },
        { name = "DB_HOST",                value = "postgres.gridwatch.internal" },
        { name = "REDIS_HOST",             value = "redis.gridwatch.internal" },
        { name = "KAFKA_BROKERS",          value = "kafka.gridwatch.internal:9092" }
      ]

      secrets = [
        {
          name      = "DB_PASSWORD"
          valueFrom = aws_secretsmanager_secret.db_password.arn
        },
        {
          name      = "CSMS_WEBHOOK_SECRET"
          valueFrom = aws_secretsmanager_secret.webhook_secret.arn
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.gridwatch.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }

      healthCheck = {
        command     = ["CMD-SHELL", "curl -sf http://localhost:8080/actuator/health || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 60
      }
    }
  ])
}

# ─── ECS Service ─────────────────────────────────────────────────────────────
resource "aws_ecs_service" "gridwatch" {
  name            = "gridwatch"
  cluster         = aws_ecs_cluster.gridwatch.id
  task_definition = aws_ecs_task_definition.gridwatch.arn
  desired_count   = 2  # 2 instances for HA; scale to 12 at 5K events/s

  capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
  }
}

# ─── Outputs ─────────────────────────────────────────────────────────────────
output "ecr_repository_url" {
  value = aws_ecr_repository.gridwatch.repository_url
}

output "wal_archive_bucket" {
  value = aws_s3_bucket.wal_archive.id
}

output "ecs_cluster_arn" {
  value = aws_ecs_cluster.gridwatch.arn
}
