resource "aws_ecr_repository" "monthly_airflow_repo" {
  name = "monthly-airflow"
  force_delete = true
}

resource "null_resource" "build_and_push_image" {
  provisioner "local-exec" {
    working_dir = "${path.module}/../airflow"
    interpreter = ["C:/Program Files/Git/bin/bash.exe", "-c"]
    command = <<-EOT
      aws ecr get-login-password --region ${var.AWS_REGION} | docker login --username AWS --password-stdin ${aws_ecr_repository.monthly_airflow_repo.repository_url} && \
      docker build -t monthly-airflow . && \
      docker tag monthly-airflow:latest ${aws_ecr_repository.monthly_airflow_repo.repository_url}:latest && \
      docker push ${aws_ecr_repository.monthly_airflow_repo.repository_url}:latest
    EOT
  }

  depends_on = [aws_ecr_repository.monthly_airflow_repo]
}

locals {
  env_vars = {
    for tuple in regexall("(.*)=(.*)", file(".env")) : tuple[0] => sensitive(tuple[1])
  }
}

resource "aws_ecs_task_definition" "ecs_task" {
  family = "monthly-airflow"
  requires_compatibilities = ["FARGATE"]
  network_mode = "awsvpc"
  cpu = "2048"
  memory = "4096"
  execution_role_arn = aws_iam_role.ecs_execution_role.arn
  task_role_arn = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name = "monthly-airflow"
      image = "${aws_ecr_repository.monthly_airflow_repo.repository_url}:latest"
      essential = true

      environment = [
        for key, value in local.env_vars : {
          name  = key
          value = value
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group" = "/ecs/monthly-airflow"
          "awslogs-region" = var.AWS_REGION
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

  depends_on = [aws_ecr_repository.monthly_airflow_repo, null_resource.build_and_push_image, aws_iam_role.ecs_execution_role, aws_iam_role.ecs_task_role, aws_cloudwatch_log_group.ecs_log_group]
}

resource "aws_cloudwatch_log_group" "ecs_log_group" {
  name = "/ecs/monthly-airflow"
  retention_in_days = 30
}

resource "aws_ecs_cluster" "ecs_cluster" {
  name = "monthly-airflow-cluster"
}

resource "aws_sfn_state_machine" "ecs_state_machine" {
  name = "ecs-monthly-airflow-state-machine"
  role_arn = aws_iam_role.sf_role.arn

  definition = jsonencode({
    Comment = "ECS State Machine for Monthly Airflow",
    StartAt = "RunEcsTask",
    States = {
      RunEcsTask = {
        Type = "Task",
        Resource = "arn:aws:states:::ecs:runTask.sync",
        TimeoutSeconds = 10800,
        Parameters = {
          Cluster = aws_ecs_cluster.ecs_cluster.id,
          TaskDefinition = aws_ecs_task_definition.ecs_task.arn,
          LaunchType = "FARGATE",
          NetworkConfiguration = {
            AwsvpcConfiguration = {
              Subnets = [aws_subnet.public_subnet.id],
              SecurityGroups = [aws_security_group.ecs_security_group.id],
              AssignPublicIp = "ENABLED"
            }
          },
          "EnableExecuteCommand" = true
        },
        End = true
      }
    }
  })

  depends_on = [aws_iam_role.sf_role, aws_ecs_task_definition.ecs_task, aws_ecs_cluster.ecs_cluster, aws_subnet.public_subnet, aws_security_group.ecs_security_group]
}