import requests
import pyodbc
import json
import datetime

# LinkedIn API credentials
CLIENT_ID = '86gawjaqysdvxi'
CLIENT_SECRET = 'JfWNGh1yETThGlVy'
ACCESS_TOKEN = ('AQWehW8PIlirSPmUTQ58qzQg-0bjbyGGvy0r5kSS3_PaCFba9Qas9eB89JfUl_wPrtiJ4cSoRcgXeY1LiZQ9kfVphw5PN4WzBDY3o4vvs0vOF2k5LcbYtjtkR53u5nVvmFjt_02Cte5c_cpcxUmK2bxkrfXcyBsxw01TMOA8oA_-Pd7IVLQjFXNwmcr5LmlIgMVS_XjBht1Q35zLYL-_kOOHCGQ3z1_y2BzfOi9vYMrEgx0yHTal2irMtYMRE00OGippdopPAFWuGfWuwfHY0ayYMHmb5PKHJG5JKM6gv1MZPg-QLNnEnHhkYw8A8IQOu1eaa7y0OqOYr9WzCcw82t4yjDJHqg')
# start_date = datetime.date(2024, 1, 1)
# end_date = datetime.date.today()

# Define the API endpoint for retrieving engagement metrics
analytics_url = f'https://api.linkedin.com/rest/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:13701784&timeIntervals.timeGranularityType=DAY&timeIntervals.timeRange.start=1710028800000&timeIntervals.timeRange.end=1716854400000'
headers = {        'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json',
    'LinkedIn-Version': '202304'
#'X-Restli-Protocol-Version': '2.0.0'
                }
# Retrieve profile information from LinkedIn API
response = requests.get(analytics_url, headers=headers)
analytics_data = response.json()

# print(analytics_data)

# Connect to SQL Server
connection_string = 'DRIVER={SQL Server};SERVER=Predicta.Database.Windows.Net;DATABASE=Predicta;UID=PredictaAdmin;PWD=Yhf^43*&^FHHytf'
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Define the INSERT query for followerCountsByStaffCountRange
insert_query = "INSERT INTO dbo.Linkedin_FollowersGain_Statistics_KS (Start_date,End_date,OrganicFollowerGain,PaidFollowerGain,OrganizationalEntity) VALUES (?, ?, ?, ?, ?)"

# Iterate over followerCountsByStaffCountRange and insert each record into the SQL table
for element in analytics_data['elements']:
    if 'followerGains' in element:
        time_range_start = datetime.datetime.fromtimestamp(element['timeRange']['start'] / 1000)
        time_range_end = datetime.datetime.fromtimestamp(element['timeRange']['end'] / 1000)
        organic_follower_gain = element['followerGains']['organicFollowerGain']
        paid_follower_gain = element['followerGains']['paidFollowerGain']
        organizational_entity = element['organizationalEntity']

        # Execute the query with the extracted values
        cursor.execute(insert_query, (time_range_start, time_range_end, organic_follower_gain, paid_follower_gain, organizational_entity))

# Commit the transaction
conn.commit()

# Close connections
cursor.close()
conn.close()
