from jira.client import JIRA
import pandas as pd
import logging
import os
import psycopg2
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

# Jira Settings
email = os.getenv('email')
api_token = os.getenv('api_token')
server = os.getenv('server')
jql = os.getenv('jql')

print("Connecting to JIRA...")
# Get issues from Jira in JSON format
jira = JIRA(options={'server': server}, basic_auth= (email, api_token))
jira_issues = jira.search_issues(jql,maxResults=0)

# JSON to pandas DataFrame
issues = pd.DataFrame()
for issue in jira_issues:
    data = {
        'Issue id':    issue.id,
        'Key':   issue.key,
        # 'self':  issue.self,

        'Assignee':        str(issue.fields.assignee),
        'Summary':         str(issue.fields.summary),
        'Status':          str(issue.fields.status.name),
        'Resolution':      str(issue.fields.resolution),

        'Bleed Start Time':     issue.fields.customfield_11445,
        'Bleed End Time':       issue.fields.customfield_11446,
        'Customer Impact':      issue.fields.customfield_11450,
        'Detection Method':     str(issue.fields.customfield_11455),
        'Detection Time':       issue.fields.customfield_11447,
        'Device Type':          str(issue.fields.customfield_11452),
        'Hardware or Software': str(issue.fields.customfield_11457),
        'Responsible Service':  str(issue.fields.customfield_11453),
        'Responsible Team':     str(issue.fields.customfield_11454),
        'Root Cause Type':      str(issue.fields.customfield_11456),
        'Severity':            str(issue.fields.customfield_11389),
    }
    issues = issues.append(data, ignore_index=True)

# DB Settings
db_name=os.getenv('db_name')
db_user=os.getenv('db_user')
db_password=os.getenv('db_password')

print("Connecting to Postgres...")
# connect to the PostgreSQL database and insert data
cnx = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@localhost:5432/{db_name}')
ses = sessionmaker(bind=cnx)
cnx.execute("DROP VIEW IF EXISTS impact_tickets_reporting;")
issues.to_sql("impact_tickets", con=cnx, index=False, if_exists="replace")

# change time fields to timestamptz type
cnx.execute("alter table public.impact_tickets alter column \"Detection Time\" type timestamptz USING \"Detection Time\"::timestamp with time zone;")
cnx.execute("alter table public.impact_tickets alter column \"Bleed Start Time\" type timestamptz USING \"Bleed Start Time\"::timestamp with time zone;")
cnx.execute("alter table public.impact_tickets alter column \"Bleed End Time\" type timestamptz USING \"Bleed End Time\"::timestamp with time zone;")

print("Creating data view...")
# create a view for the data
cnx.execute('''CREATE OR REPLACE VIEW impact_tickets_reporting AS
SELECT
"Summary",
"Key",
"Issue id",
"Bleed Start Time",
"Bleed End Time",
"Customer Impact",
"Detection Method",
"Detection Time",
"Device Type",
"Hardware or Software",
"Responsible Service",
"Responsible Team",
"Root Cause Type",
"Severity",
"Status",
"Resolution",
EXTRACT(EPOCH FROM ("Bleed End Time" - "Bleed Start Time")) / 60 as "Time to Mitigate Minutes",
ROUND(cast(EXTRACT(EPOCH FROM ("Bleed End Time" - "Bleed Start Time") / 3600) as numeric),2) as "Time to Mitigate Hours",
EXTRACT(QUARTER from ("Bleed Start Time")) as "Bleed Start Qtr",
EXTRACT(YEAR from ("Bleed Start Time")) as "Bleed Start Year",
EXTRACT(MONTH from ("Bleed Start Time")) as "Bleed Start Month",
EXTRACT(DAY from ("Bleed Start Time")) as "Bleed Start Day",
EXTRACT(EPOCH FROM ("Detection Time" - "Bleed Start Time")) / 60 as "Time to Detect Minutes",
ROUND(cast(EXTRACT(EPOCH FROM ("Detection Time" - "Bleed Start Time") / 3600) as numeric),2) as "Time to Detect Hours",
CASE
	WHEN EXTRACT(EPOCH FROM ("Detection Time" - "Bleed Start Time"))/60 < 15 then 'under 15m'
	WHEN EXTRACT(EPOCH FROM ("Detection Time" - "Bleed Start Time"))/60 >= 15
		and EXTRACT(EPOCH FROM ("Detection Time" - "Bleed Start Time"))/60 < 60 then 'between 15m and 60m'
	WHEN EXTRACT(EPOCH FROM ("Detection Time" - "Bleed Start Time"))/60 >= 60
		and EXTRACT(EPOCH FROM ("Detection Time" - "Bleed Start Time"))/60 < 12*60 then 'between 60m and 12h'
	WHEN EXTRACT(EPOCH FROM ("Detection Time" - "Bleed Start Time"))/60 >= 12*60
		and EXTRACT(EPOCH FROM ("Detection Time" - "Bleed Start Time"))/60 < 24*60 then 'between 12h and 24h'
	ELSE 'over 24h'
END "Detection Category",
CASE
	WHEN ROUND(cast(EXTRACT(EPOCH FROM ("Detection Time" - "Bleed Start Time") / 3600) as numeric),2) < 1 then 1
	WHEN ROUND(cast(EXTRACT(EPOCH FROM ("Detection Time" - "Bleed Start Time") / 3600) as numeric),2) <= 24 then ROUND(cast(EXTRACT(EPOCH FROM ("Detection Time" - "Bleed Start Time") / 3600) as numeric),0)
	WHEN ROUND(cast(EXTRACT(EPOCH FROM ("Detection Time" - "Bleed Start Time") / 3600) as numeric),2) > 24 then 25
END "Detection Hours"
FROM impact_tickets
WHERE "Responsible Service" != ''
and "Status" = 'Done'
and "Resolution" = 'Done'
and "Bleed End Time" notnull
ORDER BY "Bleed Start Time";''')

print('Exporting view to csv...')
# export table to csv
sql = "COPY (SELECT * FROM impact_tickets_reporting) TO STDOUT WITH CSV HEADER DELIMITER ','"
with open("./impact_data.csv", "w") as file:
    conn = cnx.raw_connection()
    cur = conn.cursor()
    cur.copy_expert(sql, file)

# print("Uploading csv to S3...")
# # create session with boto3
# session = boto3.Session(
# aws_access_key_id=os.getenv('aws_access_key_id'),
# aws_secret_access_key=os.getenv('aws_secret_access_key')
# )

# # create S3 Resource from the session
# s3 = session.resource('s3')

# object = s3.Object('jira-impact-data', 'impact_data.csv')

# result = object.put(Body=open('./impact_data.csv', 'rb'))

# res = result.get('ResponseMetadata')

# if res.get('HTTPStatusCode') == 200:
#     print('File Uploaded Successfully')
# else:
#     print('File Not Uploaded')

print('All done!')

