variable "region" {
    default= "eu-west-2"
}


variable "ec2_ami" {
    default= "ami-0015a39e4b7c0966f"
}

variable "s3_bucket_name" {
    default= "hrc-de-data"
}

variable "cluster_id" {
    default= "redshift-cluster-0"
}

variable "node_type" {
    default= "dc2.large"
}

variable "cluster_type" {
    default= "single-node"
}

variable "db_credentials_uname" {
    default= "awsusr"
}

variable "db_credentials_pwd" {
    default= "Mustbe8charsAndInside.EnvFile"
}

