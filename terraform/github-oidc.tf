
resource "aws_iam_openid_connect_provider" "github_actions" {
  url = "https://token.actions.githubusercontent.com"

  client_id_list = [
    "sts.amazonaws.com",
  ]

  # GitHub's OIDC thumbprint — this is the well-known, documented value.
  thumbprint_list = [
    "6938fd4e98bab03faadb97b34396831e3780aea1",
  ]
}

resource "aws_iam_role" "github_actions_ci" {
  name = "github-actions-ci-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = aws_iam_openid_connect_provider.github_actions.arn
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        }
        StringLike = {

          "token.actions.githubusercontent.com:sub" = "repo:BretaOsodo/Supply-chain-and-Package-tracking-data-pipeline:*"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "github_actions_ci_policy" {
  name = "github-actions-ci-policy"
  role = aws_iam_role.github_actions_ci.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "TerraformStateAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
        ]
        Resource = [
          "arn:aws:s3:::supply-chain-tf-state-237124340255",
          "arn:aws:s3:::supply-chain-tf-state-237124340255/*",
        ]
      },
      {
        Sid    = "TerraformLockTable"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:DeleteItem",
        ]
        Resource = "arn:aws:dynamodb:eu-north-1:*:table/supply-chain-terraform-state-locking"
      },
      {
        Sid    = "ReadForPlan"
        Effect = "Allow"
        Action = [
          "s3:GetBucket*",
          "s3:ListBucket",
          "ec2:Describe*",
          "iam:GetRole",
          "iam:GetRolePolicy",
          "iam:GetInstanceProfile",
          "iam:ListRolePolicies",
          "iam:ListAttachedRolePolicies",
          "iam:ListInstanceProfilesForRole",
        ]
        Resource = "*"
      },
      {
        Sid      = "STSIdentity"
        Effect   = "Allow"
        Action   = "sts:GetCallerIdentity"
        Resource = "*"
      }
    ]
  })
}

output "github_actions_ci_role_arn" {
  description = "Put this in GitHub repo secrets as AWS_ROLE_ARN"
  value       = aws_iam_role.github_actions_ci.arn
}
