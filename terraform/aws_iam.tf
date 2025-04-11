resource "google_iam_workload_identity_pool" "aws_wip" {
  workload_identity_pool_id = "aws-wip"
  display_name = "AWS Workload Identity Pool"
}

resource "google_iam_workload_identity_pool_provider" "aws_wip_provider" {
  workload_identity_pool_id = google_iam_workload_identity_pool.aws_wip.workload_identity_pool_id
  workload_identity_pool_provider_id = "aws-provider"
  display_name = "AWS Provider"
  
  aws {
    account_id = data.aws_caller_identity.current.account_id
  }

  depends_on = [google_iam_workload_identity_pool.aws_wip]
}

