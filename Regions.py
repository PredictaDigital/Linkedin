import requests
import pyodbc
import json
import datetime
# import sqlalchemy

# LinkedIn API credentials
CLIENT_ID = '86gawjaqysdvxi'
CLIENT_SECRET = 'JfWNGh1yETThGlVy'
ACCESS_TOKEN = ('AQW0NwO3zqAl-3Y0AeJKrbTSFNYxANRUHPteYvxIzCSl9gMjTLvxHne-z-a39PhVNF0xfuuxSJd_OGbalP_OZrVDz56OjlbeTW7rxZByml3jCg0pVnK8r6jy3oq9ZxkcZjAuKcZVYXos4sXc7HX7DmlJj3bfk5Ml8HpeTBumj9DWRDwUjWN1TWnfUULHk9L6aek6_Gs2GrwXzoMQSNVqqG9-KMRqBAw0qrBqCyyf3TJtl7Z5mf3WQePdIb_51RkvebNVAdsMqzB5_u9zHABxPw7utgtG0Yo771INJ0TFCzfJysr4_lgsyPbYBRSEuVjznun2X7wBH8m5q8eWZlnmeA_DDdnK2g')
# start_date = datetime.date(2024, 1, 1)
# end_date = datetime.date.today()

# Define the API endpoint for retrieving engagement metrics
analytics_url = f'https://api.linkedin.com/v2/regions'
headers = {        'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json',
    'LinkedIn-Version': '202304'
#'X-Restli-Protocol-Version': '2.0.0'
                }
# Retrieve profile information from LinkedIn API
response = requests.get(analytics_url, headers=headers)
analytics_data = response.json()

print(analytics_data)

# Connect to SQL Server
connection_string = 'DRIVER={SQL Server};SERVER=Predicta.Database.Windows.Net;DATABASE=Predicta;UID=PredictaAdmin;PWD=Yhf^43*&^FHHytf'
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Define the INSERT query for followerCountsByStaffCountRange
insert_query = "INSERT INTO dbo.Linkedin_Regions_KS (Locale_Country,Locale_Language,Value,Country,ID,URN, States) VALUES (?, ?, ?, ?, ?, ?, ?)"

# Iterate over followerCountsByStaffCountRange and insert each record into the SQL table
for element in analytics_data['elements']:
    if 'name' in element:
        name_data = element['name']  # Accessing the 'name' dictionary
        States_data= element['States']
        cursor.execute(insert_query, (
            name_data['locale'].get('country', ''),  # Use .get() to handle missing keys
            name_data['locale'].get('language', ''),
            name_data.get('value', ''),
            element.get('country', ''),  # Use .get() to handle missing keys
            element.get('id', ''),
            element.get('$URN', ''),
            States_data['states'].get('state', '')
        ))

# Commit the transaction
conn.commit()

# Close connections
cursor.close()
conn.close()