import os
import hashlib
import sqlite3
from datetime import datetime
from tqdm import tqdm

# Define the file extensions for pictures and videos
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff'}
VIDEO_EXTENSIONS = {'.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv'}

# Create or connect to the SQLite database
def create_connection(db_file):
    conn = sqlite3.connect(db_file)
    return conn

# Create the table for storing file information
def create_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT,
            path TEXT,
            size INTEGER,
            date_taken TEXT,
            date_saved TEXT,
            duplicate INTEGER DEFAULT 0
        )
    ''')
    conn.commit()

def hash_file(filepath):
    """Returns the SHA-1 hash of the file passed."""
    hasher = hashlib.sha1()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def find_media_files(start_path):
    media_files = {
        'images': [],
        'videos': []
    }
    
    for dirpath, _, filenames in os.walk(start_path):
        for filename in filenames:
            _, ext = os.path.splitext(filename)
            ext = ext.lower()
            
            if ext in IMAGE_EXTENSIONS:
                media_files['images'].append(os.path.join(dirpath, filename))
            elif ext in VIDEO_EXTENSIONS:
                media_files['videos'].append(os.path.join(dirpath, filename))
    
    return media_files

def get_file_info(filepath):
    """Get file size and creation date."""
    size = os.path.getsize(filepath)
    date_taken = datetime.fromtimestamp(os.path.getctime(filepath)).isoformat()
    date_saved = datetime.now().isoformat()
    return size, date_taken, date_saved

def insert_file_info(conn, file_info):
    """Insert or update file information in the database."""
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO files (hash, path, size, date_taken, date_saved, duplicate)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', file_info)
    conn.commit()

def update_duplicate_count(conn, file_hash):
    """Increment the duplicate count for a given hash."""
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE files
        SET duplicate = duplicate + 1
        WHERE hash = ?
    ''', (file_hash,))
    conn.commit()

def find_duplicates_and_store(conn, files):
    """Find duplicate files and store their information."""
    hashes = {}
    for file in tqdm(files, desc="Processing files"):
        file_hash = hash_file(file)
        size, date_taken, date_saved = get_file_info(file)

        if file_hash in hashes:
            # File is a duplicate; insert it with duplicate marked as 1
            file_info = (file_hash, file, size, date_taken, date_saved, 1)
            insert_file_info(conn, file_info)
        else:
            # New file; insert its information with duplicate marked as 0
            file_info = (file_hash, file, size, date_taken, date_saved, 0)
            insert_file_info(conn, file_info)
            hashes[file_hash] = file_hash  # Store the hash to track duplicates


# Replace 'your_directory_path' with the actual path you want to search
path_to_search = r'H:\My Pictures'  # Use a raw string to avoid escape issues
db_path = 'media_files_staging.db'  # Database file path

# Connect to the database
conn = create_connection(db_path)
create_table(conn)

# Find media files
found_media_files = find_media_files(path_to_search)

# Process images and videos for duplicates and store in the database
for media_type in ['images', 'videos']:
    find_duplicates_and_store(conn, found_media_files.get(media_type, []))

# Close the database connection
conn.close()
