resource "google_service_account" "aws_ec2_sa" {
  account_id   = "aws-ec2-sa"
  display_name  = "AWS EC2 Service Account"
}

resource "google_project_iam_member" "aws_ec2_gcs_access_iam" {
  project = var.GCP_PROJECT_ID
  role    = "roles/storage.objectUser"
  member  = "serviceAccount:${google_service_account.aws_ec2_sa.email}"

  depends_on = [google_service_account.aws_ec2_sa]
}

resource "aws_iam_role" "ec2_instance_role" {
  name = "aws-ec2-instance-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = { Service = "ec2.amazonaws.com" },
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "aws_ec2_gcp_federation_policy" {
  name = "aws-ec2-gcp-federation-policy"
  description = "IAM policy for EC2 to connect to GCP"

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

resource "aws_iam_role_policy_attachment" "ec2_instance_policy_attachment" {
  role       = aws_iam_role.ec2_instance_role.name
  policy_arn = aws_iam_policy.aws_ec2_gcp_federation_policy.arn
}

resource "google_service_account_iam_binding" "aws_ec2_wip_binding" {
  service_account_id = google_service_account.aws_ec2_sa.name
  role               = "roles/iam.workloadIdentityUser"
  members            = [
    "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.aws_wip.name}/attribute.aws_role/arn:aws:sts::${data.aws_caller_identity.current.account_id}:assumed-role/${aws_iam_role.ec2_instance_role.name}"
  ]

  depends_on = [google_service_account.aws_ec2_sa, aws_iam_role.ec2_instance_role, google_iam_workload_identity_pool.aws_wip]
}

resource "google_service_account_iam_binding" "aws_ec2_satc_binding" {
  service_account_id = google_service_account.aws_ec2_sa.name
  role               = "roles/iam.serviceAccountTokenCreator"
  members            = [
    "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.aws_wip.name}/attribute.aws_role/arn:aws:sts::${data.aws_caller_identity.current.account_id}:assumed-role/${aws_iam_role.ec2_instance_role.name}"
  ]

  depends_on = [google_service_account.aws_ec2_sa, aws_iam_role.ec2_instance_role, google_iam_workload_identity_pool.aws_wip]
}