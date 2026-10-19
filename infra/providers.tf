provider "aws" {
  region = "eu-north-1"

  default_tags {
    tags = {
      Project   = "tfl-streaming-pipeline"
      ManagedBy = "terraform"
    }
  }
}