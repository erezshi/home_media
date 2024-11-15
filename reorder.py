import os
import sqlite3
import shutil
import logging
from pathlib import Path

# Set up logging to file
logging.basicConfig(
    filename='reorder.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# File extensions for identifying photos and videos
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff'}
VIDEO_EXTENSIONS = {'.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv'}

# Define base directories for organizing photos and videos
BASE_LOCATION = r'H:\My Media'
BASE_PHOTO_DIR = os.path.join(BASE_LOCATION, 'photos')
BASE_VIDEO_DIR = os.path.join(BASE_LOCATION, 'videos')

# Connect to the media_files.db database
db_path = 'media_files_staging.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Dictionary to track moved files by hash to avoid moving duplicates
moved_files = {}

# Fetch all records from the files database
cursor.execute("SELECT path, hash, date_taken FROM files")
file_records = cursor.fetchall()

for file_path, file_hash, date_taken in file_records:
     # Skip if this file hash has already been moved
    if file_hash in moved_files:
        logging.info(f"Skipped duplicate: {file_path} (Hash: {file_hash}) already exists in database.")
        continue
    
    # Determine if the file is a photo or video based on its extension
    ext = Path(file_path).suffix.lower()
    if ext in IMAGE_EXTENSIONS:
        base_dir = BASE_PHOTO_DIR
    elif ext in VIDEO_EXTENSIONS:
        base_dir = BASE_VIDEO_DIR
    else:
        logging.warning(f"Unknown file type for {file_path}, skipping.")
        continue

    # Extract the year from the date_taken field
    year = date_taken[:4]  # Assuming date_taken is in ISO format (YYYY-MM-DD...)

    # Define the destination directory based on type and year
    destination_dir = os.path.join(base_dir, year)
    os.makedirs(destination_dir, exist_ok=True)

    # Define the destination path
    destination_path = os.path.join(destination_dir, Path(file_path).name)
    
    # Move the file
    try:
        shutil.move(file_path, destination_path)
        logging.info(f"Moved {file_path} to {destination_path}")
    except Exception as e:
        logging.error(f"Error moving file {file_path}: {e}")
        continue

    # Mark this hash as moved
    moved_files[file_hash] = destination_path

# Close the database connection
conn.close()
logging.info("File organization process completed.")
