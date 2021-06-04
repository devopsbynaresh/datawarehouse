resource "aws_glue_job" "dataexternal-model-fullload" {
  name     = "dataexternal-model-fullload"
  role_arn = data.aws_iam_role.alsacDWGlueRole.arn
    connections = [ "DATAEXTERNAL" ]
  default_arguments = {
    "--secret"               = "/dw/dw-conformed-dimensions-migration"
  }
  tags = {
      "Name" = "dataexternal-model-fullload"
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
    script_location = "s3://aws-glue-scripts-${var.account_id}-us-east-1/admin/model"
    python_version = "3"
  }
}