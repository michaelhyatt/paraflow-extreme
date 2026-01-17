terraform {
  backend "s3" {
    bucket = "mh-paraflow-terraform-state"
    key    = "shared/terraform.tfstate"
    region = "us-east-1"
  }
}
