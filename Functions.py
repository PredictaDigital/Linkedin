import requests
import pyodbc

# LinkedIn API credentials
CLIENT_ID = '86gawjaqysdvxi'
CLIENT_SECRET = 'JfWNGh1yETThGlVy'
ACCESS_TOKEN = ('AQW0NwO3zqAl-3Y0AeJKrbTSFNYxANRUHPteYvxIzCSl9gMjTLvxHne-z-a39PhVNF0xfuuxSJd_OGbalP_OZrVDz56OjlbeTW7rxZByml3jCg0pVnK8r6jy3oq9ZxkcZjAuKcZVYXos4sXc7HX7DmlJj3bfk5Ml8HpeTBumj9DWRDwUjWN1TWnfUULHk9L6aek6_Gs2GrwXzoMQSNVqqG9-KMRqBAw0qrBqCyyf3TJtl7Z5mf3WQePdIb_51RkvebNVAdsMqzB5_u9zHABxPw7utgtG0Yo771INJ0TFCzfJysr4_lgsyPbYBRSEuVjznun2X7wBH8m5q8eWZlnmeA_DDdnK2g')

# Define the API endpoint for retrieving engagement metrics
analytics_url = f'https://api.linkedin.com/v2/industries'

headers = {'Authorization': f'Bearer {ACCESS_TOKEN}',
           'Content-Type': 'application/json',
            'LinkedIn-Version': '202304'
            # 'X-Restli-Protocol-Version': '2.0.0'
           }
# Retrieve profile information from LinkedIn API
response = requests.get(analytics_url, headers=headers)
analytics_data = response.json()

# print(analytics_data)
# Connect to SQL Server
connection_string = 'DRIVER={SQL Server};SERVER=Predicta.Database.Windows.Net;DATABASE=Predicta;UID=PredictaAdmin;PWD=Yhf^43*&^FHHytf'
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Define the INSERT query for Countries
insert_query = "INSERT INTO dbo.Linkedin_Industries_KS (URN,Industry_ID,Industry_Name) VALUES (?, ?, ?)"

# Iterate over Countries and insert each record into the SQL table
for element in analytics_data['elements']:
    if 'name' in element:
        URN = element['$URN']
        Industry_ID = element['id']
        Industry_Name = element['name']['localized']['en_US']

        cursor.execute(insert_query, (URN, Industry_ID, Industry_Name))

conn.commit()

# Close connections
cursor.close()
conn.close()