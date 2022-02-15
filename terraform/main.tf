terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.27"
    }
  }

  required_version = ">= 0.14.9"
}

provider "aws" {
  profile = "default"
  region  = "${var.region}"
}

resource "aws_instance" "de-ec2" {
  ami           = "${var.ec2_ami}"
  instance_type = "t2.micro"

  tags = {
    Name = "EC2forDEprojects"
  }
}

resource "aws_s3_bucket" "de-s3" {
  bucket = "hrc-de-data"
  acl = "private"
  
  tags = {
    Name        = "S3forDEprojects "
    Environment = "Dev"
  }
}
