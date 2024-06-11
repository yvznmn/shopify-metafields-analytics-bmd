CREATE EXTERNAL SCHEMA silver
FROM DATA CATALOG
DATABASE 'dev'
IAM_ROLE 'arn:aws:iam::237320025604:role/aws-service-role/redshift.amazonaws.com/AWSServiceRoleForRedshift'
CREATE EXTERNAL DATABASE IF NOT EXISTS;