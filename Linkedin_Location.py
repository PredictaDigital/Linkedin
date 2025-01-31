import requests
import pyodbc

# LinkedIn API credentials
CLIENT_ID = '86gawjaqysdvxi'
CLIENT_SECRET = 'JfWNGh1yETThGlVy'
ACCESS_TOKEN = ('AQW0NwO3zqAl-3Y0AeJKrbTSFNYxANRUHPteYvxIzCSl9gMjTLvxHne-z-a39PhVNF0xfuuxSJd_OGbalP_OZrVDz56OjlbeTW7rxZByml3jCg0pVnK8r6jy3oq9ZxkcZjAuKcZVYXos4sXc7HX7DmlJj3bfk5Ml8HpeTBumj9DWRDwUjWN1TWnfUULHk9L6aek6_Gs2GrwXzoMQSNVqqG9-KMRqBAw0qrBqCyyf3TJtl7Z5mf3WQePdIb_51RkvebNVAdsMqzB5_u9zHABxPw7utgtG0Yo771INJ0TFCzfJysr4_lgsyPbYBRSEuVjznun2X7wBH8m5q8eWZlnmeA_DDdnK2g')

analytics_url = f'https://api.linkedin.com/rest/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:13701784'
headers = {        'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json',
    'LinkedIn-Version': '202304'
#'X-Restli-Protocol-Version': '2.0.0'
                }
# Retrieve all the Geo available for this account in Linkedin data
response = requests.get(analytics_url, headers=headers)
analytics_data = response.json()
# print(analytics_data)
geo_locations = []
for element in analytics_data['elements']:
    if 'followerCountsByGeo' in element:
        for item in element['followerCountsByGeo']:
            geo_id = item['geo']
            geo_locations.append(geo_id)

geo_ids = [geo_id.split(':')[-1] for geo_id in geo_locations]
geo_ids_str = ','.join(geo_ids)
# print(geo_locations)
# Extract geo data for the geo locations present in Linkedin data
geo_url = f'https://api.linkedin.com/v2/geo?ids=List({geo_ids_str})'
headers1 = {        'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json',
    'LinkedIn-Version': '202304',
    'X-Restli-Protocol-Version': '2.0.0'
                }
geo_response = requests.get(geo_url, headers=headers1)
geo_data = geo_response.json()

# print(geo_data)

# Connect to SQL Server
connection_string = 'DRIVER={SQL Server};SERVER=Predicta.Database.Windows.Net;DATABASE=Predicta;UID=PredictaAdmin;PWD=Yhf^43*&^FHHytf'
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Define the INSERT query for Countries
insert_query = "INSERT INTO dbo.Linkedin_Location_KS (URN,geo_id,city) VALUES (?, ?, ?)"
# Truncate the table
cursor.execute('TRUNCATE TABLE dbo.Linkedin_Location_KS')

for result_id, result_info in geo_data['results'].items():
    id_value = result_info['id']
    value = result_info['defaultLocalizedName']['value']

    cursor.execute(insert_query, ('urn:li:geo:' + str(id_value), id_value, value))
    conn.commit()

# Close connections
cursor.close()
conn.close()
