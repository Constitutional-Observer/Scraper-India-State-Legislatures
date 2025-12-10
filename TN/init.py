import subprocess
import sys
import time

def main():
    print("Starting download and upload processes...")
    
    # Start both processes
    download_process = subprocess.Popen([sys.executable, "downloader_.py"])
    #time.sleep(2)  # Let download start first
    upload_process = subprocess.Popen([sys.executable, "uploader_parallel.py"])
    
    print("Both processes running. Press Ctrl+C to stop.")
    
    try:
        # Wait for download to finish
        download_process.wait()
        print("Download finished.")
        
        # Give upload time to finish remaining files
        time.sleep(10)
        upload_process.terminate()
        print("Upload finished.")
        
    except KeyboardInterrupt:
        print("\nStopping processes...")
        download_process.terminate()
        upload_process.terminate()

if __name__ == "__main__":
    main()
