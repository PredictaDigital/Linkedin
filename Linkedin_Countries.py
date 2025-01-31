import requests
import pyodbc

# LinkedIn API credentials
APP_VERSION = '202411'
ACCESS_TOKEN = (
    'AQUhGyfzqf6s5zDC4ganhgSd2rH5vsBER_-mEm95QGwCMNxX6fUsAn_JFHpH8Rf6U5aPR8HkcA_g-MSwFCqSIMRC9bcX5dkHRBvCp1fwmFDA2vtZ7g-NxstC8b6PXMHBdHJLqiMiQj0C6f1bPkO0zGBWMdU3LOSi1c2hA5X-upa4MK-y1LyWVpe9XbNMBUGwvS7gB7U4Ivbeq5-lv7tfpmfckYh2FjImtZIG4B8JHKkaqAYrxn9xK1_A2L-Rds8eeJQB0Z7tZb-yFolupHuzTeu_nF6vqZ3OWD4MpNhyDwsNaz2JB9fTXgcTIUPxFLVxa__jOxaOr8whb5f0UdwJ_S_2pCLMBw')

# Define the API endpoint for retrieving engagement metrics
analytics_url = f'https://api.linkedin.com/v2/countries'

headers = {'Authorization': f'Bearer {ACCESS_TOKEN}',
           'Content-Type': 'application/json',
            'LinkedIn-Version': f'{APP_VERSION}'
            # 'X-Restli-Protocol-Version': '2.0.0'
           }
# Retrieve profile information from LinkedIn API
response = requests.get(analytics_url, headers=headers)
analytics_data = response.json()

# print(analytics_data)
# Connect to SQL Server
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'
db_table = "dbo.Linkedin_Countries_KS"  # need to change this to Fact table
conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# connection_string = 'DRIVER={SQL Server};SERVER=Predicta.Database.Windows.Net;DATABASE=Predicta;UID=PredictaAdmin;PWD=Yhf^43*&^FHHytf'
# conn = pyodbc.connect(connection_string)
# cursor = conn.cursor()

# Truncate the table
cursor.execute(f'TRUNCATE TABLE {db_table}')

# Define the INSERT query for Countries
insert_query = f"INSERT INTO {db_table} (Locale_Country,Locale_Language,Country_Name,Country_Group,URN,Country_Code) VALUES (?, ?, ?, ?, ?, ?)"

# Iterate over Countries and insert each record into the SQL table
for element in analytics_data['elements']:
    if 'name' in element:
        Locale_Country = element['name']['locale']['country']
        Locale_Language = element['name']['locale']['language']
        Country_Name = element['name']['value']
        Country_Group = element['countryGroup']
        URN = element['$URN']
        Country_Code = element['countryCode']

        cursor.execute(insert_query, (Locale_Country, Locale_Language, Country_Name, Country_Group, URN, Country_Code))

conn.commit()

# Close connections
cursor.close()
conn.close()
