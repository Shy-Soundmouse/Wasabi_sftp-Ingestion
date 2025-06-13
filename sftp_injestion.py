import paramiko
from datetime import 
import sqlite3

# SFTP configuration
# Connect to the SFTP Server
SFTP_HOST = 'sftp.example.com'
SFTP_PORT = 22
SFTP_USERNAME = 'your_username'
SFTP_PASSWORD = 'your_password'  # Or use SSH key

# Connect to SFTP
transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
sftp = paramiko.SFTPClient.from_transport(transport)


# List Files and Gather Metadata
remote_path = '/remote/data/folder'

# List files
file_list = sftp.listdir_attr(remote_path)

for file_attr in file_list:
    filename = file_attr.filename
    size = file_attr.st_size
    last_modified = file_attr.st_mtime  # Unix timestamp

    print(f"{filename} - {size} bytes - Last Modified: {last_modified}")

# convert the UNIX timestamp using datetime:
last_modified_date = datetime.fromtimestamp(file_attr.st_mtime).isoformat()


import sqlite3
# Insert Metadata into the Database
conn = sqlite3.connect("sftp_ingestion.db")
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS sftp_file_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT,
    last_modified TEXT,
    size INTEGER
)
''')
conn.commit()

for file_attr in file_list:
    filename = file_attr.filename
    size = file_attr.st_size
    last_modified = datetime.fromtimestamp(file_attr.st_mtime).isoformat()

    cursor.execute('''
        INSERT INTO sftp_file_metadata (filename, last_modified, size)
        VALUES (?, ?, ?)
    ''', (filename, last_modified, size))

conn.commit()
conn.close()
