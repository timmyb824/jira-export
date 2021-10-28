## Summary

A script that exports Jira ticket data, stores it in a local postgres database, performs calculations and analysis of the data. and exports the results to a CSV. Additionally, the CSV can be uploaded to AWS S3 if desired.

## Dependicies

```
brew install pyenv # python version manager
brew install pyenv-virtualenv # recommend using virtual environments
pyenv install 3.9.5 # installs python
pyenv virtualenv 3.9.5 jira-export # creates virtual environment slackbot
pyenv local jira-export # activates virtual environment
pip install -r requirements.txt # installs requirements
```

## Authentication

The following variables can be set in a .env file:
- `email`- email of the Jira user
- `api_token` - Jira API token
- `server` - Jira server hostname
- `jql` - Jira query to filter tickets
- `db_user` - database username
- `db_password` - database password
- `db_name` - database name
- `aws_access_key_id` - AWS Access Key (for uploading to AWS S3)
- `aws_secret_access_key` - AWS Secret Access Key (for uploading to AWS S3)
