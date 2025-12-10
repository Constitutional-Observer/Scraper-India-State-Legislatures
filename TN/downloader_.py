import pandas as pd
import requests
import os
import json
from urllib.parse import unquote
import re
from datetime import datetime

def extract_filename_from_preview_link(url):
    """Extract original filename from preview link"""
    try:
        if 'viewer.html' in url and 'file=' in url:
            file_path = url.split('file=')[1].split('&')[0]
            filename = file_path.split('/')[-1]
            filename = unquote(filename)
            if filename.endswith('.pdf'):
                return filename
        return None
    except:
        return None

def download_file(url, filename):
    """Download file from URL"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers, timeout=30, stream=True)
        response.raise_for_status()
        
        filepath = f"downloads/{filename}"
        os.makedirs("downloads", exist_ok=True)
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        return filepath
    except Exception as e:
        print(f"Download failed: {filename} - {e}")
        return None

def get_downloaded():
    """Get list of already downloaded files"""
    if os.path.exists('downloaded.txt'):
        with open('downloaded.txt', 'r') as f:
            return set(f.read().splitlines())
    return set()

def save_for_upload(unique_id, filename, filepath, metadata):
    """Save file info for upload"""
    os.makedirs("queue", exist_ok=True)
    
    item = {
        'unique_id': unique_id,
        'filename': filename,
        'filepath': filepath,
        'metadata': metadata
    }
    
    with open(f"queue/{unique_id}.json", 'w') as f:
        json.dump(item, f)

def main():
    df = pd.read_csv('tn_digital_data.csv', on_bad_lines="skip", engine="python")
    df = df[df['preview'].str.contains("DB", na=False)]
    downloaded = get_downloaded()
    
    print(f"Processing {len(df)} files...")
    
    for index, row in df.iterrows():
        # Get filename
        filename = extract_filename_from_preview_link(row['preview'])
        if not filename:
            filename = f"document_{index}.pdf"
        
        # Create unique ID
        unique_id = filename.replace('.pdf','').replace('(','_').replace(')','')
        unique_id = re.sub(r'[^\w\-_]', '_', unique_id)
        
        # Skip if already downloaded
        if unique_id in downloaded:
            print(f"Already downloaded: {unique_id}")
            continue
        
        print(f"Downloading: {filename}")
        
        # Download file
        filepath = download_file(row['link1'], filename)
        if not filepath:
            with open('download_failed.txt', 'a') as f:
                f.write(f"{unique_id}\n")
            continue
        
        # Detect body
        if "LA" in unique_id:
             parliament_body = "Legislative Assembly"
        elif "LC" in unique_id:
             parliament_body = "Legislative Council" 
        # Create metadata
        metadata = {
            'title': row['subject'],
            'creator': 'Tamil Nadu '+parliament_body,
            'subject': ['Tamil Nadu', parliament_body, 'Government Documents'],
            'description': f'Document from Tamil Nadu Legislative Assembly Digital Archive',
            'language': ['tamil','eng'],
            'mediatype': 'texts',
            'date': datetime.strptime(row['date'], '%d-%m-%Y').strftime("%Y-%m-%d"),
            'collection': 'tamil-nadu-legislature'
        }
        
        # Add CSV data as custom metadata with tnla_ prefix
        for col in row.index:
            if pd.notna(row[col]) and col not in ['page_start'] and not str(row[col]).startswith('http'):
                metadata[f'tnla_{col}'] = str(row[col])
        
        # Save for upload
        save_for_upload(unique_id, filename, filepath, metadata)
        
        # Mark as downloaded
        with open('downloaded.txt', 'a') as f:
            f.write(f"{unique_id}\n")
        
        print(f"✓ Downloaded: {unique_id}")
    
    print("Download complete!")

if __name__ == "__main__":
    main()
