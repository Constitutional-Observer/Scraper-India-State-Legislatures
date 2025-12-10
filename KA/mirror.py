#!/usr/bin/env python3
"""
Karnataka Legislative Assembly Mirror Tool

This script archives documents from the Karnataka Legislative Assembly to:
1. Fetch document lists for each day from 1952-06-18 to today
2. Save daily document lists as JSON files
3. Extract debate results and metadata
4. Download debate PDF documents
5. Upload to Internet Archive
"""

import os
import json
import time
import logging
import requests
import urllib3
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import urlparse
import internetarchive as ia

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mirror.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
SLEEP_BETWEEN_REQUESTS = 1.0  # seconds
BATCH_SIZE = 10  # Process dates in batches
BASE_URL = "http://103.138.196.55:9200"
LIST_API_URL = f"{BASE_URL}/api/sd/sh"
DOCUMENT_API_URL = f"{BASE_URL}/api/fs/section/debates/kla"

class KLAMirror:
    def __init__(self):
        self.session = requests.Session()
        # Disable SSL verification
        self.session.verify = False
        # Add User-Agent header
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        # Create output directories
        self.output_dir = Path('raw')
        self.list_dir = self.output_dir / 'list'
        self.document_dir = self.output_dir / 'document'
        self.metadata_dir = self.output_dir / 'metadata'
        
        for directory in [self.output_dir, self.list_dir, self.document_dir, self.metadata_dir]:
            directory.mkdir(parents=True, exist_ok=True)
            
        # Keep track of processed documents
        self.processed_file = Path('processed_documents.json')
        self.processed_docs = self.load_processed_docs()
        
        # Keep track of processed dates
        self.processed_dates_file = Path('processed_dates.json')
        self.processed_dates = self.load_processed_dates()
        
        # Search Internet Archive for existing documents
        self.search_existing_ia_documents()
        
    def load_processed_docs(self):
        """Load list of already processed documents"""
        if self.processed_file.exists():
            try:
                with open(self.processed_file, 'r') as f:
                    return set(json.load(f))
            except (json.JSONDecodeError, FileNotFoundError):
                return set()
        return set()
    
    def save_processed_docs(self):
        """Save list of processed documents"""
        with open(self.processed_file, 'w') as f:
            json.dump(list(self.processed_docs), f, indent=2)
    
    def load_processed_dates(self):
        """Load list of already processed dates"""
        if self.processed_dates_file.exists():
            try:
                with open(self.processed_dates_file, 'r') as f:
                    return set(json.load(f))
            except (json.JSONDecodeError, FileNotFoundError):
                return set()
        return set()
    
    def save_processed_dates(self):
        """Save list of processed dates"""
        with open(self.processed_dates_file, 'w') as f:
            json.dump(list(self.processed_dates), f, indent=2)
    
    def search_existing_ia_documents(self):
        """Search Internet Archive for existing KLA documents"""
        try:
            logger.info("Searching Internet Archive for existing documents...")
            
            # Search for documents with our creator
            search_query = 'creator:"Karnataka Legislative Assembly"'
            search_results = ia.search_items(search_query)
            
            existing_count = 0
            for item in search_results:
                identifier = item.get('identifier', '')
                
                # Parse identifier to extract document info
                # Expected format: kla.debates.{bookId}.{startPage}.{endPage}
                if identifier.startswith('kla.debates.'):
                    parts = identifier.split('.')
                    if len(parts) >= 5:
                        book_id = parts[2]
                        start_page = parts[3]
                        end_page = parts[4]
                        doc_key = f"{book_id}_{start_page}_{end_page}"
                        self.processed_docs.add(doc_key)
                        existing_count += 1
            
            logger.info(f"Found {existing_count} existing documents on Internet Archive")
            
        except Exception as e:
            logger.warning(f"Error searching Internet Archive: {e}")
            logger.info("Continuing without Internet Archive search results")
    
    def fetch_daily_document_list(self, date_str):
        """Fetch document list for a specific date"""
        filename = f"{date_str}.json"
        filepath = self.list_dir / filename
        
        # Skip if already exists
        if filepath.exists():
            logger.info(f"Document list already exists: {filename}")
            return filepath
        
        try:
            logger.info(f"Fetching document list for date: {date_str}")
            
            # Prepare URL with date parameters
            params = {
                'ln': '',
                'srt': '',
                'qt': 'PRC',
                'qp': '',
                'dtf': '',
                'anf': '',
                'snf': '',
                'dsubfEng': '',
                'dsubfKan': '',
                'dpfEng': '',
                'dpfKan': '',
                'dbf': '',
                'ytf': '',
                'sectionDateFrm': date_str,
                'sectionDateTo': date_str,
                'issfEng': '',
                'issfKan': '',
                'tagfKan': '',
                'tagfEng': ''
            }
            
            response = self.session.get(LIST_API_URL, params=params, timeout=30)
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            # Save to file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved document list: {filename}")
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return filepath
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching document list for {date_str}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON for {date_str}: {e}")
            return None
    
    def extract_debates_from_list(self, list_filepath):
        """Extract debate results from daily document list"""
        try:
            with open(list_filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            debates = []
            
            # Extract debateResults._source from JSON
            if 'debateResults' in data:
                for debate in data['debateResults']:
                    debates.append(debate['_source'])
                logger.info(f"Found {len(debates)} debates in {list_filepath}")
            else:
                logger.warning(f"No debateResults found in {list_filepath}")
            
            return debates
            
        except Exception as e:
            logger.error(f"Error extracting debates from {list_filepath}: {e}")
            return []
    
    def download_debate_document(self, book_id, start_page, end_page):
        """Download debate document PDF"""
        doc_key = f"{book_id}_{start_page}_{end_page}"
        filename = f"{doc_key}.pdf"
        filepath = self.document_dir / filename
        
        # Check if already exists
        if filepath.exists():
            logger.info(f"Debate document already exists: {filename}")
            return filepath
        
        try:
            url = f"{DOCUMENT_API_URL}/{book_id}/{start_page}/{end_page}"
            logger.info(f"Downloading debate document: {url}")
            
            response = self.session.get(url, timeout=60, stream=True)
            response.raise_for_status()
            
            # Check if response is actually a PDF
            content_type = response.headers.get('content-type', '')
            if 'application/pdf' not in content_type:
                logger.warning(f"Response is not a PDF for {doc_key}, content-type: {content_type}")
                return None
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded debate document: {filename}")
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return filepath
            
        except Exception as e:
            logger.error(f"Error downloading debate document {doc_key}: {e}")
            return None
    
    def save_debate_metadata(self, debate_data, book_id, start_page, end_page):
        """Save debate metadata to JSON file"""
        doc_key = f"{book_id}_{start_page}_{end_page}"
        filename = f"{doc_key}.json"
        filepath = self.metadata_dir / filename
        
        try:
            # Prepare metadata
            metadata = {
                'book_id': book_id,
                'start_page': start_page,
                'end_page': end_page,
                'source_url': f"{DOCUMENT_API_URL}/{book_id}/{start_page}/{end_page}",
                **debate_data  # Include all debate data
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved debate metadata: {filename}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving debate metadata: {e}")
            return None
    
    def upload_to_internet_archive(self, doc_key, metadata_path, pdf_path):
        """Upload debate document and metadata to Internet Archive"""
        try:
            # Create identifier for Internet Archive
            book_id, start_page, end_page = doc_key.split('_')
            identifier = f"karnatakalegislativeassembly.debates.{book_id}.{start_page}.{end_page}"
            
            # Prepare metadata for Internet Archive
            ia_metadata = {
                'creator': 'Karnataka Legislative Assembly Secretariat',
                'source': f'{DOCUMENT_API_URL}/{book_id}/{start_page}/{end_page}',
                'mediatype': 'texts',
                'language': ['English', 'Kannada'],
                'licenseurl': 'http://creativecommons.org/licenses/publicdomain/',
                'subject': ['Karnataka Legislative Assembly'],
            }
            
            # Add extracted metadata
            if metadata_path and metadata_path.exists():
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    doc_metadata = json.load(f)

                ia_metadata['title'] = doc_metadata['debate_subject_kan']
                ia_metadata['description'] = 'Karnataka Legislative Assembly Debates - Book ' + book_id + ', Pages ' + start_page + '-' + end_page
                ia_metadata['date'] = doc_metadata['debate_section_date']
                
                # Add specific fields from debate metadata
                for key, value in doc_metadata.items():
                    if key not in ['book_id', 'start_page', 'end_page', 'source_url']:
                        ia_metadata[f"kla_{key}"] = value

            # Convert lists to strings joined with commas, except for Language and License
            for key, value in ia_metadata.items():
                if isinstance(value, list) and key not in ['language', 'subject']:
                    ia_metadata[key] = ', '.join(value)
            
            # Prepare files to upload
            files = []
            if pdf_path and pdf_path.exists():
                files.append(str(pdf_path))
            
            if files:
                logger.info(f"Uploading to Internet Archive: {identifier}")
                logger.info(f"Metadata: {ia_metadata}")
                logger.info(f"Files: {files}")
                
                item = ia.get_item(identifier)
                item.upload(files, metadata=ia_metadata, verbose=True, verify=True, checksum=True, retries=3, retries_sleep=10)
                
                logger.info(f"Successfully uploaded: {identifier}")
                return True
            else:
                logger.warning(f"No files to upload for {identifier}")
                return False
                
        except Exception as e:
            logger.error(f"Error uploading to Internet Archive: {e}")
            return False
    
    def check_document_exists(self, book_id, start_page, end_page):
        """Check if document files already exist for the given metadata"""
        doc_key = f"{book_id}_{start_page}_{end_page}"
        
        # Check if PDF file exists
        pdf_filename = f"{doc_key}.pdf"
        pdf_filepath = self.document_dir / pdf_filename
        
        # Check if metadata file exists
        metadata_filename = f"{doc_key}.json"
        metadata_filepath = self.metadata_dir / metadata_filename
        
        # Check if already processed
        already_processed = doc_key in self.processed_docs
        
        # Log the status
        if already_processed:
            logger.info(f"Document already processed: {doc_key}")
        elif pdf_filepath.exists() and metadata_filepath.exists():
            logger.info(f"Document files already exist: {doc_key} (PDF: {pdf_filename}, Metadata: {metadata_filename})")
        elif pdf_filepath.exists():
            logger.info(f"PDF file already exists: {doc_key} (PDF: {pdf_filename})")
        elif metadata_filepath.exists():
            logger.info(f"Metadata file already exists: {doc_key} (Metadata: {metadata_filename})")
        
        return {
            'exists': already_processed or (pdf_filepath.exists() and metadata_filepath.exists()),
            'already_processed': already_processed,
            'pdf_exists': pdf_filepath.exists(),
            'metadata_exists': metadata_filepath.exists(),
            'pdf_path': pdf_filepath,
            'metadata_path': metadata_filepath
        }
    
    def check_archive_org_exists(self, book_id, start_page, end_page):
        """Check if document is already archived on archive.org"""
        try:
            # Create the expected identifier format
            identifier = f"karnatakalegislativeassembly.debates.{book_id}.{start_page}.{end_page}"
            
            logger.info(f"Checking if document exists on archive.org: {identifier}")
            
            # Try to get the item from Internet Archive
            item = ia.get_item(identifier)
            
            # Check if the item exists and has files
            if item.exists and item.files:
                logger.info(f"Document already exists on archive.org: {identifier}")
                return True
            else:
                logger.info(f"Document not found on archive.org: {identifier}")
                return False
                
        except Exception as e:
            logger.warning(f"Error checking archive.org for {book_id}_{start_page}_{end_page}: {e}")
            # If we can't check, assume it doesn't exist to be safe
            return False
    
    def process_debate(self, debate_data, date_str):
        """Process a single debate: extract metadata, download PDF, upload to IA"""
        try:
            # Extract required fields from debate data
            book_id = debate_data.get('bookId')
            start_page = debate_data.get('startPage')
            end_page = debate_data.get('endPage')
            
            if not all([book_id, start_page, end_page]):
                logger.warning(f"Missing required fields in debate data: {debate_data}")
                return False
            
            doc_key = f"{book_id}_{start_page}_{end_page}"
            
            # Step 1: Check if document is already archived on archive.org
            if self.check_archive_org_exists(book_id, start_page, end_page):
                logger.info(f"Document already archived on archive.org: {doc_key}")
                # Mark as processed to avoid re-checking
                self.processed_docs.add(doc_key)
                self.save_processed_docs()
                return True
            
            # Step 2: Check if document files already exist locally
            doc_status = self.check_document_exists(book_id, start_page, end_page)
            
            if doc_status['already_processed']:
                logger.info(f"Debate already processed: {doc_key}")
                return True
            
            if doc_status['exists']:
                logger.info(f"Document files already exist for {doc_key}, proceeding with upload")
                # Use existing files for upload
                pdf_path = doc_status['pdf_path'] if doc_status['pdf_exists'] else None
                metadata_path = doc_status['metadata_path'] if doc_status['metadata_exists'] else None
                
                # If metadata doesn't exist, create it
                if not metadata_path:
                    debate_data['processing_date'] = date_str
                    metadata_path = self.save_debate_metadata(debate_data, book_id, start_page, end_page)
                
                # If PDF doesn't exist, download it
                if not pdf_path:
                    pdf_path = self.download_debate_document(book_id, start_page, end_page)
                    if not pdf_path:
                        logger.warning(f"Failed to download PDF for {doc_key}")
                        return False
                
                # Upload to Internet Archive
                upload_success = self.upload_to_internet_archive(doc_key, metadata_path, pdf_path)
                
                # Mark as processed if successful
                if upload_success:
                    self.processed_docs.add(doc_key)
                    self.save_processed_docs()
                    logger.info(f"Successfully processed debate: {doc_key}")
                
                return upload_success
            
            logger.info(f"Processing new debate: {doc_key}")
            
            # Add date to metadata
            debate_data['processing_date'] = date_str
            
            # Step 3: Save metadata
            metadata_path = self.save_debate_metadata(debate_data, book_id, start_page, end_page)
            
            # Step 4: Download PDF
            pdf_path = self.download_debate_document(book_id, start_page, end_page)
            if not pdf_path:
                logger.warning(f"Failed to download PDF for {doc_key}")
                return False
            
            # Step 5: Upload to Internet Archive
            upload_success = self.upload_to_internet_archive(doc_key, metadata_path, pdf_path)
            
            # Mark as processed if successful
            if upload_success:
                self.processed_docs.add(doc_key)
                self.save_processed_docs()
                logger.info(f"Successfully processed debate: {doc_key}")
            
            return upload_success
            
        except Exception as e:
            logger.error(f"Error processing debate: {e}")
            return False
    
    def process_date(self, date_str):
        """Process all debates for a specific date"""
        # Skip if already processed
        if date_str in self.processed_dates:
            logger.info(f"Date already processed: {date_str}")
            return True
        
        logger.info(f"Processing date: {date_str}")
        
        # Step 1: Fetch daily document list
        list_path = self.fetch_daily_document_list(date_str)
        if not list_path:
            logger.warning(f"Failed to fetch document list for {date_str}")
            return False
        
        # Step 2: Extract debates from list
        debates = self.extract_debates_from_list(list_path)
        if not debates:
            logger.info(f"No debates found for {date_str}")
            # Mark as processed even if no debates found
            self.processed_dates.add(date_str)
            self.save_processed_dates()
            return True
        
        # Step 3: Process each debate
        success_count = 0
        for debate in debates:
            if self.process_debate(debate, date_str):
                success_count += 1
        
        logger.info(f"Successfully processed {success_count}/{len(debates)} debates for {date_str}")
        
        # Mark date as processed
        self.processed_dates.add(date_str)
        self.save_processed_dates()
        
        return success_count > 0
    
    def generate_date_range(self, start_date_str='1952-06-18'):
        """Generate date range from start_date_str to today
        
        Args:
            start_date_str: Start date in YYYY-MM-DD format (default: 1952-06-18)
        """
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.now()
        
        current_date = start_date
        while current_date <= end_date:
            yield current_date.strftime('%Y-%m-%d')
            current_date += timedelta(days=1)
    
    def run(self):
        """Main execution function"""
        logger.info("Starting Karnataka Legislative Assembly Mirror Tool")
        
        try:
            # Generate all dates from 1952-06-18 to today
            dates = list(self.generate_date_range())
            logger.info(f"Generated {len(dates)} dates to process (starting from 1952-06-18)")
            
            # Filter out already processed dates
            remaining_dates = [date for date in dates if date not in self.processed_dates]
            logger.info(f"Remaining dates to process: {len(remaining_dates)}")
            
            # Process dates in batches
            for i in range(0, len(remaining_dates), BATCH_SIZE):
                batch = remaining_dates[i:i + BATCH_SIZE]
                logger.info(f"Processing batch {i//BATCH_SIZE + 1}/{(len(remaining_dates) + BATCH_SIZE - 1)//BATCH_SIZE}")
                
                for date_str in batch:
                    success = self.process_date(date_str)
                    
                    # Save progress periodically
                    if (i + batch.index(date_str) + 1) % 10 == 0:
                        self.save_processed_dates()
                        self.save_processed_docs()
                
                logger.info(f"Completed batch ending with {batch[-1]}")
                
                # Clean up files periodically to save disk space
                if i % 50 == 0:
                    self.cleanup_files()
            
            logger.info("Mirror tool completed")
            
        except KeyboardInterrupt:
            logger.info("Process interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.save_processed_dates()
            self.save_processed_docs()
    
    def cleanup_files(self):
        """Clean up downloaded files to save disk space"""
        try:
            logger.info("Cleaning up downloaded files...")
            
            # Delete document files but keep metadata
            for file in self.document_dir.glob('*.pdf'):
                file.unlink()
            
            # Delete list files older than 7 days
            cutoff_date = datetime.now() - timedelta(days=7)
            for file in self.list_dir.glob('*.json'):
                if file.stat().st_mtime < cutoff_date.timestamp():
                    file.unlink()
            
            logger.info("Cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


def main():
    """Main entry point"""
    mirror = KLAMirror()
    mirror.run()


if __name__ == "__main__":
    main()
