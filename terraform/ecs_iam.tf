
resource "google_service_account" "aws_ecs_sa" {
  account_id = "aws-ecs-sa"
  display_name = "AWS GCP Service Account"
}

resource "google_project_iam_member" "aws_ecs_gcs_access_iam" {
  project = var.GCP_PROJECT_ID
  role = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.aws_ecs_sa.email}"

  depends_on = [google_service_account.aws_ecs_sa]
}

resource "google_project_iam_member" "aws_ecs_bigquery_access_iam" {
  project = var.GCP_PROJECT_ID
  role = "roles/bigquery.admin"
  member = "serviceAccount:${google_service_account.aws_ecs_sa.email}"

  depends_on = [google_service_account.aws_ecs_sa]
}

resource "google_project_iam_member" "aws_ecs_dataproc_access_iam" {
  project = var.GCP_PROJECT_ID
  role = "roles/dataproc.serverlessEditor"
  member = "serviceAccount:${google_service_account.aws_ecs_sa.email}"

  depends_on = [google_service_account.aws_ecs_sa]
}

resource "google_project_iam_member" "aws_ecs_service_account_user" {
  project = var.GCP_PROJECT_ID
  role = "roles/iam.serviceAccountUser"
  member = "serviceAccount:${google_service_account.aws_ecs_sa.email}"

  depends_on = [google_service_account.aws_ecs_sa]
}

resource "google_iam_workload_identity_pool" "aws_ecs_wip" {
  workload_identity_pool_id = "aws-ecs-wip"
  display_name = "AWS ECS Workload Identity Pool"
}

resource "google_iam_workload_identity_pool_provider" "aws_ecs_wip_provider" {
  workload_identity_pool_id = google_iam_workload_identity_pool.aws_ecs_wip.workload_identity_pool_id
  workload_identity_pool_provider_id = "aws-provider"
  display_name = "AWS Provider"
  description =  "AWS Provider for ECS"
  
  aws {
    account_id = data.aws_caller_identity.current.account_id
  }

  depends_on = [google_iam_workload_identity_pool.aws_ecs_wip]
}

resource "aws_iam_role" "ecs_execution_role" {
  name = "aws-ecs-execution-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = { Service = "ecs-tasks.amazonaws.com" },
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_execution_policy_attachment" {
  role       = aws_iam_role.ecs_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}


resource "aws_iam_role" "ecs_task_role" {
  name = "aws-ecs-task-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = { Service = "ecs-tasks.amazonaws.com" },
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "aws_ecs_gcp_federation_policy" {
  name        = "aws-ecs-gcp-federation-policy"
  description = "IAM policy for ECS to connect to GCP"
  
  policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "sts:GetCallerIdentity",
          "sts:GetServiceBearerToken",
          "sts:AssumeRoleWithWebIdentity"
        ],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_task_role_gcp_federation_attachment" {
  role       = aws_iam_role.ecs_task_role.name
  policy_arn = aws_iam_policy.aws_ecs_gcp_federation_policy.arn

  depends_on = [aws_iam_role.ecs_task_role, aws_iam_policy.aws_ecs_gcp_federation_policy]
}

resource "aws_iam_policy" "ecs_exec_policy" {
  name        = "ecs-exec-policy"
  description = "IAM policy for ECS Exec"

  policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "ssmmessages:CreateControlChannel",
          "ssmmessages:CreateDataChannel",
          "ssmmessages:OpenControlChannel",
          "ssmmessages:OpenDataChannel"
        ],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_exec_policy_attachment" {
  role       = aws_iam_role.ecs_task_role.name
  policy_arn = aws_iam_policy.ecs_exec_policy.arn

  depends_on = [aws_iam_role.ecs_task_role, aws_iam_policy.ecs_exec_policy]
}

resource "google_service_account_iam_binding" "aws_ecs_wip_binding" {
  service_account_id = google_service_account.aws_ecs_sa.name
  role = "roles/iam.workloadIdentityUser"
  members = [
    "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.aws_ecs_wip.name}/attribute.aws_role/arn:aws:sts::${data.aws_caller_identity.current.account_id}:assumed-role/${aws_iam_role.ecs_task_role.name}"
  ]

  depends_on = [ aws_iam_role.ecs_task_role, google_service_account.aws_ecs_sa, google_iam_workload_identity_pool.aws_ecs_wip ]
}

resource "google_service_account_iam_binding" "aws_ecs_satc_binding" {
  service_account_id = google_service_account.aws_ecs_sa.name
  role = "roles/iam.serviceAccountTokenCreator"
  members = [
    "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.aws_ecs_wip.name}/attribute.aws_role/arn:aws:sts::${data.aws_caller_identity.current.account_id}:assumed-role/${aws_iam_role.ecs_task_role.name}"
  ]

  depends_on = [ aws_iam_role.ecs_task_role, google_service_account.aws_ecs_sa, google_iam_workload_identity_pool.aws_ecs_wip ]
}

resource "aws_iam_role" "sf_role" {
  name = "aws-ecs-sf-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = { Service = "states.amazonaws.com" },
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "sf_policy" {
  name        = "aws-ecs-sf-policy"
  description = "IAM policy for Step Functions to start and stop ECS tasks"

  policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "ecs:RunTask",
          "ecs:StopTask",
          "events:PutTargets",  
          "events:PutRule",
          "events:DescribeRule"
        ]
        Resource = "*"
      }, 
      {
        Effect   = "Allow",
        Action   = [
          "iam:PassRole"
        ],
        Resource = [
          aws_iam_role.ecs_execution_role.arn,
          aws_iam_role.ecs_task_role.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "sf_role_policy_attachment" {
  role       = aws_iam_role.sf_role.name
  policy_arn = aws_iam_policy.sf_policy.arn

  depends_on = [aws_iam_role.sf_role, aws_iam_policy.sf_policy]
}