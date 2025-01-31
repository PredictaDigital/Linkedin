import requests
from datetime import datetime
import pyodbc
import json

ACCESS_TOKEN = ('AQWehW8PIlirSPmUTQ58qzQg-0bjbyGGvy0r5kSS3_PaCFba9Qas9eB89JfUl_wPrtiJ4cSoRcgXeY1LiZQ9kfVphw5PN4WzBDY3o4vvs0vOF2k5LcbYtjtkR53u5nVvmFjt_02Cte5c_cpcxUmK2bxkrfXcyBsxw01TMOA8oA_-Pd7IVLQjFXNwmcr5LmlIgMVS_XjBht1Q35zLYL-_kOOHCGQ3z1_y2BzfOi9vYMrEgx0yHTal2irMtYMRE00OGippdopPAFWuGfWuwfHY0ayYMHmb5PKHJG5JKM6gv1MZPg-QLNnEnHhkYw8A8IQOu1eaa7y0OqOYr9WzCcw82t4yjDJHqg')
Organization_URN = 'urn:li:organization:13701784'
# Define the URL for fetching posts
posts_url = 'https://api.linkedin.com/rest/posts?author=urn:li:organization:13701784&q=author&count=100&sortBy=LAST_MODIFIED'
headers = {
    'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json',
    'LinkedIn-Version': '202404'
}
# Fetch posts
response = requests.get(posts_url, headers=headers)
posts_data = response.json()
# print(posts_data)
share_ids = [post['id'] for post in posts_data['elements'] if post['id'].startswith('urn:li:share')]
ugcpost_ids = [post['id'] for post in posts_data['elements'] if post['id'].startswith('urn:li:ugcPost')]

# print(ugcpost_ids)
insights = {}
for share_id in share_ids:
    insights_url = f'https://api.linkedin.com/rest/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity={Organization_URN}&shares={share_id}'
    response = requests.get(insights_url, headers=headers)
    insights_data = response.json()
    insights[share_id] = insights_data

# Fetch insights for each ugcPost ID
for ugcpost_id in ugcpost_ids:
    insights_url = f'https://api.linkedin.com/rest/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity={Organization_URN}&ugcPosts={ugcpost_id}'
    response = requests.get(insights_url, headers=headers)
    insights_data = response.json()
    insights[ugcpost_id] = insights_data

for post in posts_data['elements']:
    post_id = post['id']
    post['insights'] = insights.get(post_id, {})

    # print(post)

# Connect to SQL Server
connection_string = 'DRIVER={SQL Server};SERVER=Predicta.Database.Windows.Net;DATABASE=Predicta;UID=PredictaAdmin;PWD=Yhf^43*&^FHHytf'
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Truncate the table
cursor.execute('TRUNCATE TABLE dbo.Linkedin_Posts_Statistics_KS')

# Function to convert timestamp to datetime
def convert_to_datetime(timestamp):
    return datetime.fromtimestamp(timestamp / 1000.0) if timestamp else None

# Transform the data to fit the required columns
def transform_post_data(post):
    # Extract insights
    insights_elements = post['insights'].get('elements', [])
    total_share_stats = insights_elements[0].get('totalShareStatistics', {}) if insights_elements else {}
    organizational_entity = insights_elements[0].get('organizationalEntity', '') if insights_elements else ''

    # Calculate engagement rate
    # engagement = total_share_stats.get('engagement', 0)*100
    clickCount = total_share_stats.get('clickCount', 0)
    impression_count = total_share_stats.get('impressionCount', 0)
    # engagement_rate = engagement / impression_count if impression_count else 0
    ClickThrough_rate = clickCount / impression_count if impression_count else 0
    # Extract distribution data
    distribution = post.get('distribution', {})

    transformed = {
        'isReshareDisabledByAuthor': post.get('isReshareDisabledByAuthor'),
        'createdAt': convert_to_datetime(post.get('createdAt')),
        'lifecycleState': post.get('lifecycleState'),
        'lastModifiedAt': convert_to_datetime(post.get('lastModifiedAt')),
        'visibility': post.get('visibility'),
        'publishedAt': convert_to_datetime(post.get('publishedAt')),
        'author': post.get('author'),
        'id': post.get('id'),
        'parent': post.get('reshareContext', {}).get('parent'),
        'root': post.get('reshareContext', {}).get('root'),
        'feedDistribution': distribution.get('feedDistribution'),
        # Store as JSON string
        'commentary': post.get('commentary', ''),
        'uniqueImpressionsCount': total_share_stats.get('uniqueImpressionsCount', 0),
        'shareCount': total_share_stats.get('shareCount', 0),
        'engagement': total_share_stats.get('clickCount', 0)+total_share_stats.get('commentCount', 0)+total_share_stats.get('likeCount', 0)+ total_share_stats.get('shareCount', 0),
        'clickCount': total_share_stats.get('clickCount', 0),
        'likeCount': total_share_stats.get('likeCount', 0),
        'impressionCount': total_share_stats.get('impressionCount', 0),
        'commentCount': total_share_stats.get('commentCount', 0),
        'engagementRate': total_share_stats.get('engagement', 0)*100,
        'ClickThrough_rate' : ClickThrough_rate * 100,
        'isEditedByAuthor' : post['lifecycleStateInfo'].get('isEditedByAuthor',''),
        'organizationalEntity' : organizational_entity
    }
    return transformed


# Debugging function to print post data
# def print_post_data(post):
#     print(json.dumps(post, indent=4))


# Print post data for debugging
# for post in posts_data['elements']:
#     print_post_data(post)

for post in posts_data['elements']:
    transformed_post = transform_post_data(post)
    cursor.execute('''
      INSERT INTO dbo.Linkedin_Posts_Statistics_KS (
       PostID,OrganizationalEntity,Author,Commentary,IsReshareDisabledByAuthor,CreatedAt,LifecycleState,LastModifiedAt,
        Visibility,PublishedAt,FeedDistribution,IsEditedByAuthor,UniqueImpressionsCount
        ,ShareCount,Engagement,ClickCount,LikeCount,CommentCount,ImpressionCount,EngagementRate,ClickThroughRate
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      ''', (
        transformed_post['id'],
        transformed_post['organizationalEntity'],
        transformed_post['author'],
        transformed_post['commentary'],
        transformed_post['isReshareDisabledByAuthor'],
        transformed_post['createdAt'],
        transformed_post['lifecycleState'],
        transformed_post['lastModifiedAt'],
        transformed_post['visibility'],
        transformed_post['publishedAt'],
        transformed_post['feedDistribution'],
        transformed_post['isEditedByAuthor'],
        transformed_post['uniqueImpressionsCount'],
        transformed_post['shareCount'],
        transformed_post['engagement'],
        transformed_post['clickCount'],
        transformed_post['likeCount'],
        transformed_post['commentCount'],
        transformed_post['impressionCount'],
        transformed_post['engagementRate'],
        transformed_post['ClickThrough_rate']
    ))

conn.commit()
conn.close()
