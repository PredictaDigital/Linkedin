import requests
import pyodbc
import json
import datetime

# LinkedIn API credentials
CLIENT_ID = '86gawjaqysdvxi'
CLIENT_SECRET = 'JfWNGh1yETThGlVy'
ACCESS_TOKEN = ('AQWehW8PIlirSPmUTQ58qzQg-0bjbyGGvy0r5kSS3_PaCFba9Qas9eB89JfUl_wPrtiJ4cSoRcgXeY1LiZQ9kfVphw5PN4WzBDY3o4vvs0vOF2k5LcbYtjtkR53u5nVvmFjt_02Cte5c_cpcxUmK2bxkrfXcyBsxw01TMOA8oA_-Pd7IVLQjFXNwmcr5LmlIgMVS_XjBht1Q35zLYL-_kOOHCGQ3z1_y2BzfOi9vYMrEgx0yHTal2irMtYMRE00OGippdopPAFWuGfWuwfHY0ayYMHmb5PKHJG5JKM6gv1MZPg-QLNnEnHhkYw8A8IQOu1eaa7y0OqOYr9WzCcw82t4yjDJHqg')

# Define the API endpoint for retrieving engagement metrics
analytics_url = f'https://api.linkedin.com/rest/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:13701784'
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
# Truncate the table
cursor.execute('TRUNCATE TABLE dbo.Linkedin_Followers_KS')

# Define the INSERT query for followerCountsByStaffCountRange
insert_query = "INSERT INTO dbo.Linkedin_Followers_KS (OrganicFollowerCount,PaidFollowerCount,DataType,DataType_ID) VALUES (?, ?, ?, ?)"

# Iterate over followerCountsByStaffCountRange and insert each record into the SQL table
for element in analytics_data['elements']:
    if 'followerCountsByStaffCountRange' in element:
        for item in element['followerCountsByStaffCountRange']:
            cursor.execute(insert_query, (
            item['followerCounts']['organicFollowerCount'], item['followerCounts']['paidFollowerCount'],
            "staffCountRange", item['staffCountRange']))

    if 'followerCountsByFunction' in element:
        for item in element['followerCountsByFunction']:
            cursor.execute(insert_query, (
            item['followerCounts']['organicFollowerCount'], item['followerCounts']['paidFollowerCount'], "Function",
            item['function']))

    if 'followerCountsBySeniority' in element:
        for item in element['followerCountsBySeniority']:
            cursor.execute(insert_query, (
            item['followerCounts']['organicFollowerCount'], item['followerCounts']['paidFollowerCount'], "Seniority",
            item['seniority']))

    if 'followerCountsByAssociationType' in element:
        for item in element['followerCountsByAssociationType']:
            cursor.execute(insert_query, (
            item['followerCounts']['organicFollowerCount'], item['followerCounts']['paidFollowerCount'],
            "AssociationType", item['associationType']))

    if 'followerCountsByIndustry' in element:
        for item in element['followerCountsByIndustry']:
            cursor.execute(insert_query, (
            item['followerCounts']['organicFollowerCount'], item['followerCounts']['paidFollowerCount'], "Industry",
            item['industry']))

    if 'followerCountsByGeo' in element:
        for item in element['followerCountsByGeo']:
            cursor.execute(insert_query, (
            item['followerCounts']['organicFollowerCount'], item['followerCounts']['paidFollowerCount'], "Location",
            item['geo']))

    if 'followerCountsByGeoCountry' in element:
        for item in element['followerCountsByGeoCountry']:
            cursor.execute(insert_query, (
            item['followerCounts']['organicFollowerCount'], item['followerCounts']['paidFollowerCount'], "Country",
            item['geo']))

# Commit the transaction
conn.commit()

# Close connections
cursor.close()
conn.close()

