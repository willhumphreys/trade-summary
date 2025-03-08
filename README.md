
Register job definition in AWS


```bash
# First, create the substituted file
sed "s|\${ACCOUNT_ID}|$(aws sts get-caller-identity --query "Account" --output text --profile default)|g" aws-definitions/job-definition.json > /tmp/processed-job-definition.json

# Then register it
aws batch register-job-definition --cli-input-json file:///tmp/processed-job-definition.json
``