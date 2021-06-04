#### PROVIDER INFO ####
provider "aws" {
  region = var.region
  version = "~> 2.56"
  profile = "saml"
}

#### TERRAFORM BACKEND ####
# This sets up the backend for Terraform to ensure the state file is centralized and doesn't get corrupted
# if multiple developers are deploying these Terraform scripts.
terraform {
  backend "s3" {
    profile = "saml"
  }
}

data "aws_subnet" "subnet1" {
    filter {
      name = "tag:Name"
      values = ["sn-${var.env}-priv-1a"]
    }
}

data "aws_subnet" "subnet2" {
    filter {
      name = "tag:Name"
      values = ["sn-${var.env}-priv-1b"]
    }
}

data "aws_subnet" "subnet3" {
    filter {
      name = "tag:Name"
      values = ["sn-${var.env}-data-1b"]
    }
}

data "aws_vpc" "vpc" {
    filter {
      name = "tag:Name"
      values = ["vpc-use1-${var.env}"]
    }
}

data "aws_security_group" "sg_glue" {
  id = var.security_group_id
}

data "aws_iam_role" "alsacLambdaPrdS3Access" {
  name = "alsacLambdaPrdS3Access"
}

data "aws_iam_role" "alsacLambdaStructuredS3Access" {
  name = "alsacLambdaStructuredS3Access"
}

data "aws_iam_role" "alsacStepTriggerLambda" {
  name = "alsacStepTriggerLambda"
}

data "aws_iam_role" "alsacGluePrdS3Access" {
  name = "alsacGluePrdS3Access"
}

data "aws_s3_bucket" "glue_scripts_bucket" {
  bucket = "aws-glue-scripts-${var.account_id}-us-east-1"
}

data "aws_s3_bucket" "structured_bucket" {
  bucket = "alsac-dev-dla-structured"
}

data "aws_iam_role" "alsacDWGlueRole" {
  name = "alsacDWGlueRole"
}