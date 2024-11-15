import os
import hashlib
import sqlite3
from datetime import datetime
from tqdm import tqdm
from PIL import Image
from PIL.ExifTags import TAGS
from hachoir.parser import createParser
from hachoir.metadata import extractMetadata
from pathlib import Path
import logging


# Set up logging to file
logging.basicConfig(
    filename='getmedia_info.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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
    """Get file size and the actual date taken from metadata, if available."""
    size = os.path.getsize(filepath)
    date_taken = None

    # Check if the file is an image and try to read the EXIF date taken
    ext = Path(filepath).suffix.lower()
    if ext in IMAGE_EXTENSIONS:
        try:
            with Image.open(filepath) as img:
                exif_data = img._getexif()
                if exif_data:
                    for tag, value in exif_data.items():
                        tag_name = TAGS.get(tag, tag)
                        if tag_name == 'DateTimeOriginal':  # Common EXIF tag for date taken
                            date_taken = value
                            break
        except Exception as e:
            logging.info(f"Error reading EXIF data for {filepath}: {e}")

    # If it's a video, try to read the metadata date
    elif ext in VIDEO_EXTENSIONS:
        try:
            parser = createParser(filepath)
            if parser:
                metadata = extractMetadata(parser)
                if metadata and metadata.has("creation_date"):
                    date_taken = metadata.get("creation_date").strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logging.error(f"Error reading video metadata for {filepath}: {e}")

    # Fallback to creation date if no metadata date is found
    if not date_taken:
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
    """Find duplicate files and store their information, comparing against the database."""
    cursor = conn.cursor()
    hashes = {}

    for file in tqdm(files, desc="Processing files"):
        file_hash = hash_file(file)
        size, date_taken, date_saved = get_file_info(file)

        cursor.execute("SELECT COUNT(*) FROM files WHERE path = ?", (file,))
        exists = cursor.fetchone()[0] > 0

        if exists:
            logging.info(f"Skipped duplicate file with the same path: {file} (Hash: {file_hash}) already exists in database.")
            continue


        # Check if the hash already exists in the database
        cursor.execute('SELECT hash FROM files WHERE hash = ?', (file_hash,))
        db_entry = cursor.fetchone()

        if db_entry:
            # File is a duplicate; insert it with duplicate marked as 1
            file_info = (file_hash, file, size, date_taken, date_saved, 1)
            insert_file_info(conn, file_info)
        elif file_hash in hashes:
            # File is a duplicate within the current set of files
            file_info = (file_hash, file, size, date_taken, date_saved, 1)
            insert_file_info(conn, file_info)
        else:
            # New file; insert its information with duplicate marked as 0
            file_info = (file_hash, file, size, date_taken, date_saved, 0)
            insert_file_info(conn, file_info)
            hashes[file_hash] = file_hash  # Store the hash to track duplicates



# Replace 'your_directory_path' with the actual path you want to search
# path_to_search = r'H:\My Pictures'  # Use a raw string to avoid escape issues

paths_to_search = [r'H:\My Media']
db_path = 'media_files_staging.db'  # Database file path

# Connect to the database
conn = create_connection(db_path)
create_table(conn)

# Find media files from multiple paths
all_media_files = {'images': [], 'videos': []}
for path in paths_to_search:
    found_media_files = find_media_files(path)
    all_media_files['images'].extend(found_media_files['images'])
    all_media_files['videos'].extend(found_media_files['videos'])

# Process images and videos for duplicates and store them in the database
for media_type in ['images', 'videos']:
    find_duplicates_and_store(conn, all_media_files.get(media_type, []))

# Close the database connection
conn.close()
