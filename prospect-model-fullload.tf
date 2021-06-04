resource "aws_glue_job" "prospect-model-fullload" {
  name     = "prospect-model-fullload"
  role_arn = data.aws_iam_role.alsacDWGlueRole.arn
    connections = [ "PROSPECT" ]
  default_arguments = {
    "--secret"               = "/dw/dim-prospect-migration"
  }
  tags = {
    "Name" = "prospect-model-fullload"
    "Compliance"  = "PII"
    "Environment" = "${var.env}"
    "CostCenter" = "5760 4560 5418"
    "ContactEmail"= "imran.mohammed@stjude.org"
    "Application" = "data-warehouse"
    "CreateDate" = "04-26-2021"
  }
  glue_version = "2.0"
  number_of_workers = "2"
  worker_type = "Standard"



  command {
    script_location = "s3://aws-glue-scripts-${var.account_id}-us-east-1/admin/prospect"
    python_version = "3"
  }
}