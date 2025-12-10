import os
import json
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from internetarchive import upload

# Thread-safe locks
uploaded_lock = Lock()
failed_lock = Lock()

def upload_to_ia(filepath, ia_identifier, metadata):
    """Upload file to Internet Archive"""
    try:
        print(f"Uploading {ia_identifier}...")
        response = upload(ia_identifier, filepath, metadata=metadata)
        return response[0].status_code == 200, None
    except Exception as e:
        return False, str(e)

def mark_uploaded(unique_id):
    with uploaded_lock:
        with open('uploaded.txt', 'a') as f:
            f.write(f"{unique_id}\n")

def mark_failed(unique_id):
    with failed_lock:
        with open('upload_failed.txt', 'a') as f:
            f.write(f"{unique_id}\n")

def get_uploaded():
    if os.path.exists('uploaded.txt'):
        with open('uploaded.txt', 'r') as f:
            return set(f.read().splitlines())
    return set()

def process_file(queue_file, uploaded_set):
    """Process a single file upload"""
    try:
        with open(queue_file, 'r') as f:
            item = json.load(f)
        
        unique_id = item['unique_id']
        filepath = item['filepath']
        metadata = item['metadata']
        
        # Skip if already uploaded or file doesn't exist
        if unique_id in uploaded_set or not os.path.exists(filepath):
            os.remove(queue_file)
            return
        
        # Upload
        success, error_msg = upload_to_ia(filepath, unique_id, metadata)
        
        # Try with modified ID if duplicate
        #if not success and error_msg and ("access denied" in error_msg.lower() or "duplicate" in error_msg.lower()):
        #    modified_id = unique_id + "_"
        #    success, _ = upload_to_ia(filepath, modified_id, metadata)
        #    if success:
        #        unique_id = modified_id
        
        if success:
            print(f"✓ Uploaded: {unique_id}")
            mark_uploaded(unique_id)
        else:
            print(f"✗ Failed: {unique_id}")
            print(error_msg)
            mark_failed(unique_id)
        
        os.remove(queue_file)
        
    except Exception as e:
        print(f"Error processing {queue_file}: {e}")
        try:
            os.remove(queue_file)
        except:
            pass

def main():
    MAX_WORKERS = 10
    
    # Get files and uploaded list
    queue_files = glob.glob("queue/*.json")
    uploaded = get_uploaded()
    
    if not queue_files:
        print("No files in queue")
        return
    
    print(f"Processing {len(queue_files)} files with {MAX_WORKERS} workers")
    
    # Process all files in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_file, f, uploaded) for f in queue_files]
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Task error: {e}")
    
    print("All uploads completed")

if __name__ == "__main__":
    main()
