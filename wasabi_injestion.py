import boto3
import sqlite3


#  Connect to Wasabi

# Wasabi configuration
WASABI_ACCESS_KEY = 'your_access_key'
WASABI_SECRET_KEY = 'your_secret_key'
WASABI_REGION = 'us-east-1'  # Example region
WASABI_ENDPOINT = 'https://s3.wasabisys.com'
WASABI_BUCKET = 'your-bucket-name'

# Create S3 client
s3 = boto3.client(
    's3',
    endpoint_url=WASABI_ENDPOINT,
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    region_name=WASABI_REGION
)

response = s3.list_objects_v2(Bucket=WASABI_BUCKET)

for obj in response.get('Contents', []):
    print(f"File: {obj['Key']}, Last Modified: {obj['LastModified']}, Size: {obj['Size']}")



conn = sqlite3.connect("ingestion.db")
cursor = conn.cursor()

# Create a table for metadata if not exists
cursor.execute('''
CREATE TABLE IF NOT EXISTS file_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT,
    last_modified TEXT,
    size INTEGER
)
''')
conn.commit()

# Record File Metadata in the Database
for obj in response.get('Contents', []):
    filename = obj['Key']
    last_modified = obj['LastModified'].isoformat()
    size = obj['Size']

    cursor.execute('''
        INSERT INTO file_metadata (filename, last_modified, size)
        VALUES (?, ?, ?)
    ''', (filename, last_modified, size))

conn.commit()
conn.close()
