# IAM Role-to-Policy Mapping

Maps each IAM policy in this repo to the role it attaches to, where the file is saved, and its purpose.

## Design principles

* S3 access is scoped per role (least privilege) — each role only reaches the prefixes it needs.
* CloudWatch logging is one reusable customer-managed policy, attached across roles rather than duplicated inline.
* Every Glue role trusts `glue.amazonaws.com` to assume it (trust policy, separate from permissions).
* PassRole is held by the launching identity, not the job roles.

## Role / policy mapping

| Role | Attached policy | File path in repo | Purpose |
|---|---|---|---|
| AWS_Full_data_S3_read_write_ETL | AWS_Full_data_S3_read_write_ETL_Policy | iam_roles/etl_glue_iam/AWS_Full_data_S3_read_write_ETL | Cleaning job: reads raw CSV (raw_data_full), writes cleaned parquet (full_data_parquet) |
| AWS_LAMBDA_S3_TRIGGER_GLUE_ETL_SPARK_JOB | AWS_LAMBDA_S3_TRIGGER_GLUE_ETL_SPARK_JOB_Policy | AWS_LAMBDA_S3_TRIGGER_GLUE_ETL_SPARK_JOB | Lambda that triggers the Glue ETL job when data lands in S3 |
| AWS_ROLE_READ_S3_WRITE_S3_GLUE_ETL_SPARK | AWS_ROLE_READ_S3_WRITE_S3_GLUE_ETL_SPARK_Policy | AWS_ROLE_READ_S3_WRITE_S3_GLUE_ETL_SPAR | Earlier version of the tier ETL role |
| AWS_ROLE_READ_S3_WRITE_S3_GLUE_ETL_SPARK_Policy_V2 | AwsGlueSessionUserRestrictedNotebookPolicy | Straight from AWS perms | AWS-managed: enables interactive notebook sessions |
| AWS_ROLE_READ_S3_WRITE_S3_GLUE_ETL_SPARK_Policy_V2 | AwsGlueSessionUserRestrictedPolicy | Straight from AWS perms | AWS-managed: enables interactive Glue sessions |
| AWS_ROLE_READ_S3_WRITE_S3_GLUE_ETL_SPARK_Policy_V2 | log_read_role_ETL | Global_Cloud_Watch_low_perm Info| CloudWatch logging for the tier ETL job |
| AWS_ROLE_READ_S3_WRITE_S3_GLUE_ETL_SPARK_Policy_V2 | loyalty_tier_write | AWS_ROLE_READ_S3_WRITE_S3_GLUE_ETL_SPARK_Policy_V2 | Tier job: reads full_data_parquet, writes loyalty-tier |
| AWS_ROLE_READ_S3_WRITE_S3_GLUE_ETL_SPARK_Policy_V2 | PassRole_Policy_Tier_ | PassRole_Policy_Tier_ | Allows this role to be passed to Glue |
| AWS_S3_Trigger_GLUE_ETL_REACT | AWS_S3_Trigger_GLUE_ETL_REACT_Policy | AWS_S3_Trigger_GLUE_ETL_REACT | S3 event trigger permissions for kicking off Glue ETL |
| AWS_TEST_ROLE | AmazonS3FullAccess | Straight from AWS perms | AWS-managed (broad dev role — see notes) |
| AWS_TEST_ROLE | AWSGlueConsoleFullAccess | Straight from AWS perms | AWS-managed (broad dev role — see notes) |
| AWS_TEST_ROLE | AWSGlueConsoleRoleInlinePolicy-read-only-specific-access | AWS_S3_Trigger_GLUE_ETL_REACT | Inline read-only access |
| AWS_TEST_ROLE | CloudWatchLogsFullAccess | Straight from AWS perms | AWS-managed (broad dev role — see notes) |
| AWS_TEST_ROLE | passrole | PassRole_Policy_Tier_ | Allows passing roles to Glue |

## Shared / reusable policies

* **Global_Cloud_Watch_low_perm** — customer-managed CloudWatch logging policy (CreateLogGroup / CreateLogStream / PutLogEvents scoped to `/aws-glue/*`). Attached to the ETL roles so run logs can be viewed in CloudWatch.

## Notes

* **AWS_TEST_ROLE** is a broad development/experimentation role, deliberately not least-privilege. It holds high-level permissions to learn how AWS works and to temporarily bypass role blockers during development. The production job roles above use scoped, least-privilege policies — the test role is not intended for production use.
* The `_Policy_V2` (tier ETL) role includes Glue interactive-session permissions used during development; a production batch-job version would not need these.

