import requests
import pyodbc

# LinkedIn API credentials
CLIENT_ID = '86gawjaqysdvxi'
CLIENT_SECRET = 'JfWNGh1yETThGlVy'
ACCESS_TOKEN = ('AQWehW8PIlirSPmUTQ58qzQg-0bjbyGGvy0r5kSS3_PaCFba9Qas9eB89JfUl_wPrtiJ4cSoRcgXeY1LiZQ9kfVphw5PN4WzBDY3o4vvs0vOF2k5LcbYtjtkR53u5nVvmFjt_02Cte5c_cpcxUmK2bxkrfXcyBsxw01TMOA8oA_-Pd7IVLQjFXNwmcr5LmlIgMVS_XjBht1Q35zLYL-_kOOHCGQ3z1_y2BzfOi9vYMrEgx0yHTal2irMtYMRE00OGippdopPAFWuGfWuwfHY0ayYMHmb5PKHJG5JKM6gv1MZPg-QLNnEnHhkYw8A8IQOu1eaa7y0OqOYr9WzCcw82t4yjDJHqg')

# Define the API endpoint for retrieving engagement metrics
analytics_url = f'https://api.linkedin.com/v2/seniorities'

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

# Truncate the table
cursor.execute('TRUNCATE TABLE dbo.Linkedin_Seniorities_KS')
# Define the INSERT query for Countries
insert_query = "INSERT INTO dbo.Linkedin_Seniorities_KS (URN,Seniority_Id,Seniority_Name) VALUES (?, ?, ?)"

# Iterate over Countries and insert each record into the SQL table
for element in analytics_data['elements']:
    if 'name' in element:
        URN = element['$URN']
        Seniority_Id = element['id']
        Seniority_Name = element['name']['localized']['en_US']

        cursor.execute(insert_query, (URN, Seniority_Id, Seniority_Name))

conn.commit()

# Close connections
cursor.close()
conn.close()