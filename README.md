# aws-csv-processor

## Pre-requisites

## Setting up AWS account
* Create user with sufficient privileges for deployment
* CDK bootstrap `cdk bootstrap`

## Local development
### Prerequisites
* AWS CDK installed
* Python 3.6+ installed
* Python venv installed
* Docker installed (we're using a construct for Lambda which requires this locally)
* AWS credentials configured

### Python venv setup
* Be in root folder of this repo
* `python3 -m venv .venv`
* `source .venv/bin/activate`
* `pip install -r requirements.txt`
* You're now running in your local dev environment

### Pre-commit hooks
Pre-commmit hooks are used to ensure quality of committed code. Documentation for this can be found here: https://pre-commit.com/
Pre-commit configuration can be found in `.pre-commit-config.yaml`

This can also be run manually to check all files from within the venv: `pre-commit run --all-files`

### Deploying from local
* NOTE - `cdk bootstrap` needs to have been run first for this to work
* Run `cdk synth` to check that cdk setup is valid
* Run `cdk deploy` to deploy using the AWS credentials you have configured.
** If using a profile then do `cdk deploy --profile {profile_name}`
