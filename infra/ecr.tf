locals {
  images = ["tfl-stream", "tfl-batch"]
}

resource "aws_ecr_repository" "images" {
  for_each = toset(local.images)

  name                 = each.key
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  image_scanning_configuration {
    scan_on_push = true
  }
}

# Storage costs $0.10/GB-month and the batch image is ~1 GB, so keep only the last few.
resource "aws_ecr_lifecycle_policy" "images" {
  for_each   = aws_ecr_repository.images
  repository = each.value.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the last 3 images"
      selection = {
        tagStatus   = "any",
        countType   = "imageCountMoreThan",
        countNumber = 3
      }
      action = { type = "expire" }
    }]
  })
}