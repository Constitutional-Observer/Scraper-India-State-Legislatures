#!/usr/bin/env python3
"""
UP Legislative Assembly Mirror Tool

This script archives documents from the UP Legislative Assembly to:
1. Fetch proceedings table data via API
2. Parse JSON response to extract document metadata and URLs
3. Download legislative proceedings PDF documents
4. Upload to Internet Archive with proper metadata
"""

import os
import json
import time
import logging
import requests
import urllib3
import re
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, urljoin
import internetarchive as ia

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
urllib3_logger = logging.getLogger('urllib3')
urllib3_logger.setLevel(logging.CRITICAL)

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
BASE_URL = "https://uplegisassembly.gov.in"
API_URL = f"{BASE_URL}/angular.asmx/Download_Proceedings_Table"
PDF_BASE_URL = f"{BASE_URL}/getImageHandler.ashx"
NAME_TO_NUMBER = {'First': 1, 'Second': 2, 'Third': 3, 'Fourth': 4, 'Fifth': 5, 'Sixth': 6, 'Seventh': 7, 'Eighth': 8, 'Ninth': 9, 'Tenth': 10,
'Eleventh': 11, 'Twelfth': 12, 'Thirteenth': 13, 'Fourteenth': 14, 'Fifteenth': 15, 'Sixteenth': 16, 'Seventeenth': 17, 'Eighteenth': 18, 'Nineteenth': 19, 'Twentieth': 20,
'Election': 'Election of Speaker', 'Eight': 8}

class UPLegislatureMirror:
    def __init__(self):
        self.session = requests.Session()
        # Disable SSL verification
        self.session.verify = False
        # Add headers for UP Legislative Assembly API
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json;charset=utf-8',
            'Origin': BASE_URL,
            'Referer': f'{BASE_URL}/Karyawahi/Proceeding_Synopsis_en.aspx'
        })
        
        # Create output directories
        self.output_dir = Path('raw')
        self.document_dir = self.output_dir / 'document'
        self.metadata_dir = self.output_dir / 'metadata'
        
        for directory in [self.output_dir, self.document_dir, self.metadata_dir]:
            directory.mkdir(parents=True, exist_ok=True)
            
        # Keep track of processed documents
        self.processed_file = Path('processed_documents.json')
        self.processed_docs = self.load_processed_docs()
        
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
    
    
    def search_existing_ia_documents(self):
        """Search Internet Archive for existing UP Legislature documents"""
        try:
            logger.info("Searching Internet Archive for existing documents...")
            
            # Search for documents with our creator
            search_query = 'creator:"Uttar Pradesh Legislative Assembly"'
            search_results = ia.search_items(search_query)
            
            existing_count = 0
            for item in search_results:
                identifier = item.get('identifier', '')
                
                # Parse identifier to extract document info
                # Expected format: uplegislature.{valueId}
                if identifier.startswith('uplegislature.'):
                    # Mark as processed to avoid re-uploading
                    self.processed_docs.add(identifier)
                    existing_count += 1
            
            logger.info(f"Found {existing_count} existing documents on Internet Archive")
            
        except Exception as e:
            logger.warning(f"Error searching Internet Archive: {e}")
            logger.info("Continuing without Internet Archive search results")
    
    def fetch_proceedings_table(self, force_refetch=False):
        """Fetch proceedings table data via API or load from existing file"""
        table_file = Path('table.json')
        
        # Check if table.json already exists and we're not forcing a refetch
        if not force_refetch and table_file.exists():
            try:
                logger.info("Loading proceedings table from existing table.json")
                with open(table_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logger.info(f"Successfully loaded existing table.json ({table_file.stat().st_size} bytes)")
                return data
            except (json.JSONDecodeError, FileNotFoundError) as e:
                logger.warning(f"Error loading existing table.json: {e}")
                logger.info("Falling back to fetching from API...")
        
        # Fetch from API if file doesn't exist, force_refetch is True, or loading failed
        try:
            logger.info("Fetching proceedings table from API...")
            
            # API payload
            payload = {
                "parm1": "proceeding_download",
                "parm2": "",
                "parm3": "",
                "parm4": ""
            }
            
            response = self.session.post(API_URL, json=payload, timeout=60)
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            # Save to file
            with open(table_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved proceedings table: {table_file}")
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching proceedings table: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response: {e}")
            return None
    
    def parse_proceedings_data(self, data):
        """Parse proceedings JSON data to extract document metadata"""
        try:
            documents = []
            
            # Extract documents from the 'd' array
            proceedings = data.get('d', [])
            logger.info(f"Found {len(proceedings)} proceedings entries")
            
            for entry in proceedings:
                # Skip entries without valueId (needed for PDF URL)
                value_id = entry.get('valueId')
                if not value_id:
                    continue
                
                # Extract required metadata
                assembly_name = entry.get('assembly_name_english', '')
                date = entry.get('date', '')
                session_year = entry.get('session_year', '')
                session_name = entry.get('Session_name_english', '')

                # Get assembly number from assembly name
                assembly_number = assembly_name.split(' ')[0]
                assembly_number = NAME_TO_NUMBER[assembly_number]

                # Get session number from session name
                session_number = session_name.split(' ')[0]
                session_number = NAME_TO_NUMBER[session_number]
                
                # Skip entries without essential data
                if not all([assembly_name, date, session_year]):
                    logger.debug(f"Skipping entry with missing data: valueId={value_id}")
                    continue
                
                # Create document object
                document = {
                    'valueId': value_id,
                    'assembly_name': assembly_name,
                    'assembly_number': assembly_number,
                    'date': date,
                    'session_year': session_year,
                    'session_name': session_name or '',
                    'session_number': session_number,
                    'pdf_url': f"{PDF_BASE_URL}?ID={value_id}&con=2",
                    'identifier': f"uplegislature.assembly{assembly_number}.session{session_number}.{date}",
                    'filename': f"{date}.pdf"
                }
                
                documents.append(document)
            
            logger.info(f"Found {len(documents)} valid documents with PDFs")
            return documents
            
        except Exception as e:
            logger.error(f"Error parsing proceedings data: {e}")
            return []
    
    def download_document(self, document):
        """Download a legislative document PDF"""
        filename = document['filename']
        filepath = self.document_dir / filename
        
        # Check if already exists
        if filepath.exists():
            logger.info(f"Document already exists: {filename}")
            return filepath
        
        try:
            url = document['pdf_url']
            logger.info(f"Downloading document: {filename} from {url}")
            
            # Use a separate session for PDF downloads to avoid header conflicts
            pdf_session = requests.Session()
            pdf_session.verify = False
            pdf_session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': f'{BASE_URL}/Karyawahi/Proceeding_Synopsis_en.aspx',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'
            })
            
            response = pdf_session.get(url, timeout=60, stream=True)
            response.raise_for_status()
            
            # Check if response is actually a PDF
            content_type = response.headers.get('content-type', '')
            if 'application/pdf' not in content_type:
                logger.warning(f"Response is not a PDF for {filename}, content-type: {content_type}")
            
            # Download the content
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Check file size to determine if it's likely a valid PDF
            file_size = filepath.stat().st_size
            if file_size < 5000:  # Less than 5KB is likely an error page
                logger.error(f"Downloaded file too small ({file_size} bytes), likely an error page: {filename}")
                filepath.unlink()  # Remove the invalid file
                return None
            
            logger.info(f"Downloaded document: {filename} ({file_size} bytes)")
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return filepath
            
        except Exception as e:
            logger.error(f"Error downloading document {filename}: {e}")
            return None
    
    def save_document_metadata(self, document):
        """Save document metadata to JSON file"""
        filename = f"{document['filename'].replace('.pdf', '')}.json"
        filepath = self.metadata_dir / filename
        
        try:
            # Prepare metadata with processing timestamp
            metadata = {
                **document,
                'processing_date': datetime.now().isoformat(),
                'source_url': document['pdf_url']
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved document metadata: {filename}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving document metadata: {e}")
            return None
    
    def upload_to_internet_archive(self, document, metadata_path, pdf_path):
        """Upload document and metadata to Internet Archive"""
        try:
            # Create identifier for Internet Archive
            identifier = document['identifier']
            
            # Prepare metadata for Internet Archive
            ia_metadata = {
                'creator': 'Uttar Pradesh Legislative Assembly',
                'source': document['pdf_url'],
                'mediatype': 'texts',
                'language': ['Hindi', 'English'],
                'licenseurl': 'http://creativecommons.org/licenses/publicdomain/',
                'subject': ['Uttar Pradesh Legislative Assembly'],
                'collection': 'parliamentofindia'
            }
            
            # Add extracted metadata
            if metadata_path and metadata_path.exists():
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    doc_metadata = json.load(f)

                ia_metadata['title'] = f"Assembly {document['assembly_number']}, Session {document['session_number']}, {document['date']}"
                ia_metadata['description'] = f"Uttar Pradesh Legislative Assembly proceedings for Assembly {document['assembly_number']}, Session {document['session_number']} on {document['date']}"
                
                # Set date
                ia_metadata['date'] = document['date']
                
                # Add specific fields from document metadata
                for key, value in doc_metadata.items():
                    if key not in ['pdf_url', 'source_url', 'processing_date', 'filename', 'valueId', 'date', 'identifier']:
                        ia_metadata[f"up_legislature_{key}"] = str(value)

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
    
    def check_document_exists(self, document):
        """Check if document files already exist for the given document"""
        identifier = document['identifier']
        
        # Check if PDF file exists
        pdf_filename = document['filename']
        pdf_filepath = self.document_dir / pdf_filename
        
        # Check if metadata file exists
        metadata_filename = f"{document['filename'].replace('.pdf', '')}.json"
        metadata_filepath = self.metadata_dir / metadata_filename
        
        # Check if already processed
        already_processed = identifier in self.processed_docs
        
        # Log the status
        if already_processed:
            logger.info(f"Document already processed: {identifier}")
        elif pdf_filepath.exists() and metadata_filepath.exists():
            logger.info(f"Document files already exist: {identifier} (PDF: {pdf_filename}, Metadata: {metadata_filename})")
        elif pdf_filepath.exists():
            logger.info(f"PDF file already exists: {identifier} (PDF: {pdf_filename})")
        elif metadata_filepath.exists():
            logger.info(f"Metadata file already exists: {identifier} (Metadata: {metadata_filename})")
        
        return {
            'exists': already_processed or (pdf_filepath.exists() and metadata_filepath.exists()),
            'already_processed': already_processed,
            'pdf_exists': pdf_filepath.exists(),
            'metadata_exists': metadata_filepath.exists(),
            'pdf_path': pdf_filepath,
            'metadata_path': metadata_filepath
        }
    
    def check_archive_org_exists(self, document):
        """Check if document is already archived on archive.org"""
        try:
            # Use the identifier directly
            identifier = document['identifier']
            
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
            logger.warning(f"Error checking archive.org for {document['identifier']}: {e}")
            # If we can't check, assume it doesn't exist to be safe
            return False
    
    def process_document(self, document):
        """Process a single document: extract metadata, download PDF, upload to IA"""
        try:
            identifier = document['identifier']
            
            # Step 1: Check if document is already archived on archive.org
            if self.check_archive_org_exists(document):
                logger.info(f"Document already archived on archive.org: {identifier}")
                # Mark as processed to avoid re-checking
                self.processed_docs.add(identifier)
                self.save_processed_docs()
                return True
            
            # Step 2: Check if document files already exist locally
            doc_status = self.check_document_exists(document)
            
            if doc_status['already_processed']:
                logger.info(f"Document already processed: {identifier}")
                return True
            
            if doc_status['exists']:
                logger.info(f"Document files already exist for {identifier}, proceeding with upload")
                # Use existing files for upload
                pdf_path = doc_status['pdf_path'] if doc_status['pdf_exists'] else None
                metadata_path = doc_status['metadata_path'] if doc_status['metadata_exists'] else None
                
                # If metadata doesn't exist, create it
                if not metadata_path:
                    metadata_path = self.save_document_metadata(document)
                
                # If PDF doesn't exist, download it
                if not pdf_path:
                    pdf_path = self.download_document(document)
                    if not pdf_path:
                        logger.warning(f"Failed to download PDF for {identifier}, skipping document")
                        return False
                
                # Upload to Internet Archive
                upload_success = self.upload_to_internet_archive(document, metadata_path, pdf_path)
                
                # Mark as processed if successful
                if upload_success:
                    self.processed_docs.add(identifier)
                    self.save_processed_docs()
                    logger.info(f"Successfully processed document: {identifier}")
                
                return upload_success
            
            logger.info(f"Processing new document: {identifier}")
            
            # Step 3: Save metadata
            metadata_path = self.save_document_metadata(document)
            
            # Step 4: Download PDF
            pdf_path = self.download_document(document)
            if not pdf_path:
                logger.warning(f"Failed to download PDF for {identifier}, skipping document")
                return False
            
            # Step 5: Upload to Internet Archive
            upload_success = self.upload_to_internet_archive(document, metadata_path, pdf_path)
            
            # Mark as processed if successful
            if upload_success:
                self.processed_docs.add(identifier)
                self.save_processed_docs()
                logger.info(f"Successfully processed document: {identifier}")

                # Delete the PDF file
                pdf_path.unlink()
                logger.info(f"Deleted PDF file: {pdf_path}")

                # Delete the metadata file
                metadata_path.unlink()
                logger.info(f"Deleted metadata file: {metadata_path}")
            
            return upload_success
            
        except Exception as e:
            logger.error(f"Error processing document: {e}")
            return False
    
    def process_all_documents(self, force_refetch=False):
        """Process all documents from the proceedings table"""
        logger.info("Starting to process all UP Legislature documents")
        
        # Step 1: Fetch proceedings table data (will use existing table.json if available)
        data = self.fetch_proceedings_table(force_refetch=force_refetch)
        if not data:
            logger.error("Failed to fetch or load proceedings table data")
            return False
        
        # Step 2: Parse data to extract documents
        documents = self.parse_proceedings_data(data)
        if not documents:
            logger.error("No documents found in proceedings data")
            return False
        
        logger.info(f"Found {len(documents)} documents to process")
        
        # Step 3: Process each document in reverse order (last to first)
        success_count = 0
        total_count = len(documents)
        
        # Reverse the documents list to process from last to first
        documents.reverse()
        
        for i, document in enumerate(documents, 1):
            logger.info(f"Processing document {i}/{total_count}: {document['identifier']}")
            
            if self.process_document(document):
                success_count += 1
            
            # Save progress periodically
            if i % 10 == 0:
                self.save_processed_docs()
                logger.info(f"Progress: {i}/{total_count} documents processed, {success_count} successful")
        
        logger.info(f"Processing complete: {success_count}/{total_count} documents successfully processed")
        return success_count > 0
    
    def run(self, force_refetch=False):
        """Main execution function"""
        logger.info("Starting UP Legislative Assembly Mirror Tool")
        
        try:
            # Process all documents from the proceedings table
            success = self.process_all_documents(force_refetch=force_refetch)
            
            if success:
                logger.info("Mirror tool completed successfully")
            else:
                logger.error("Mirror tool completed with errors")
            
        except KeyboardInterrupt:
            logger.info("Process interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.save_processed_docs()


def main():
    """Main entry point"""
    import sys
    
    # Check for force refetch flag from command line or environment
    force_refetch = False
    if '--force-refetch' in sys.argv or os.environ.get('FORCE_REFETCH', '').lower() in ('true', '1', 'yes'):
        force_refetch = True
        logger.info("Force refetch enabled - will fetch fresh data from API")
    
    mirror = UPLegislatureMirror()
    mirror.run(force_refetch=force_refetch)


if __name__ == "__main__":
    main()
