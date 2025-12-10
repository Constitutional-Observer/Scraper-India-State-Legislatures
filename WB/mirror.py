#!/usr/bin/env python3
"""
West Bengal Legislature Mirror Tool

This script archives documents from the West Bengal Legislature to:
1. Fetch assembly and council proceedings from JSON API endpoints
2. Extract document metadata and download URLs
3. Download legislative debate PDF documents
4. Upload to Internet Archive with proper metadata
5. Process West Bengal Legislative Assembly and Council proceedings
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
from bs4 import BeautifulSoup
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
BASE_URL = "https://lalib.wb.gov.in"
ASSEMBLY_API_URL = f"{BASE_URL}/showProceedingsRecordList?sEcho=2&iColumns=7&sColumns=,,,,,,&iDisplayStart=0&iDisplayLength=500&mDataProp_0=&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=true&mDataProp_1=&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&mDataProp_2=&sSearch_2=&bRegex_2=false&bSearchable_2=true&bSortable_2=true&mDataProp_3=&sSearch_3=&bRegex_3=false&bSearchable_3=true&bSortable_3=true&mDataProp_4=&sSearch_4=&bRegex_4=false&bSearchable_4=true&bSortable_4=true&mDataProp_5=&sSearch_5=&bRegex_5=false&bSearchable_5=true&bSortable_5=true&mDataProp_6=&sSearch_6=&bRegex_6=false&bSearchable_6=true&bSortable_6=true&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1"
COUNCIL_API_URL = f"{BASE_URL}/showProceedingsRecordList?sEcho=2&iColumns=7&sColumns=,,,,,,&iDisplayStart=0&iDisplayLength=500&mDataProp_0=&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=true&mDataProp_1=&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&mDataProp_2=&sSearch_2=&bRegex_2=false&bSearchable_2=true&bSortable_2=true&mDataProp_3=&sSearch_3=&bRegex_3=false&bSearchable_3=true&bSortable_3=true&mDataProp_4=&sSearch_4=&bRegex_4=false&bSearchable_4=true&bSortable_4=true&mDataProp_5=&sSearch_5=&bRegex_5=false&bSearchable_5=true&bSortable_5=true&mDataProp_6=&sSearch_6=&bRegex_6=false&bSearchable_6=true&bSortable_6=true&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&_"

class WestBengalLegislatureMirror:
    def __init__(self):
        self.session = requests.Session()
        # Disable SSL verification
        self.session.verify = False
        # Add User-Agent header
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'cross-site',
            'Priority': 'u=0, i',
            'Pragma': 'no-cache'
        })
        
        # Create output directories
        self.output_dir = Path('raw')
        self.tree_dir = self.output_dir / 'tree'
        self.document_dir = self.output_dir / 'document'
        self.metadata_dir = self.output_dir / 'metadata'
        
        for directory in [self.output_dir, self.tree_dir, self.document_dir, self.metadata_dir]:
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
    
    
    def load_change_proceedings_page(self):
        """Load the change proceedings page to get necessary cookies"""
        try:
            logger.info("Loading change proceedings page to get cookies...")
            change_url = f"{BASE_URL}/showChangeProceedings"
            
            response = self.session.get(change_url, timeout=30)
            response.raise_for_status()
            
            logger.info(f"Successfully loaded change proceedings page, got {len(self.session.cookies)} cookies")
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error loading change proceedings page: {e}")
            return False

    def search_existing_ia_documents(self):
        """Search Internet Archive for existing West Bengal Legislature documents"""
        try:
            logger.info("Searching Internet Archive for existing documents...")
            
            # Search for documents with our creator
            search_query = 'creator:"West Bengal State Legislature"'
            search_results = ia.search_items(search_query)
            
            existing_count = 0
            for item in search_results:
                identifier = item.get('identifier', '')
                
                # Parse identifier to extract document info
                # Expected format: westbengallegislature.{house}.{proceeding_id}
                if identifier.startswith('westbengallegislature.'):
                    # Mark as processed to avoid re-uploading
                    self.processed_docs.add(identifier)
                    existing_count += 1
            
            logger.info(f"Found {existing_count} existing documents on Internet Archive")
            
        except Exception as e:
            logger.warning(f"Error searching Internet Archive: {e}")
            logger.info("Continuing without Internet Archive search results")
    
    def fetch_proceedings_data(self, house_type):
        """Fetch proceedings data from JSON API"""
        if house_type == "assembly":
            url = ASSEMBLY_API_URL
            filename = "assembly_proceedings.json"
        elif house_type == "council":
            url = COUNCIL_API_URL
            filename = "council_proceedings.json"
        else:
            logger.error(f"Invalid house type: {house_type}")
            return None
            
        filepath = self.tree_dir / filename
        
        # Check if file already exists
        if filepath.exists():
            logger.info(f"{house_type} proceedings file already exists: {filename}")
            return filepath
        
        try:
            logger.info(f"Fetching {house_type} proceedings data...")

            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            # Save JSON to file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {house_type} proceedings: {filename}")
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return filepath
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {house_type} proceedings: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response for {house_type}: {e}")
            return None
    
    def parse_proceedings_json(self, json_filepath, house_type):
        """Parse the proceedings JSON to extract document structure and URLs"""
        try:
            with open(json_filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            documents = []
            
            # Check if the JSON has the expected structure
            if 'data' not in data:
                logger.error(f"No 'data' field found in {house_type} proceedings JSON")
                return []
            
            proceedings_list = data['data']
            logger.info(f"Processing {len(proceedings_list)} {house_type} proceedings...")
            
            for proceeding in proceedings_list:
                if not isinstance(proceeding, dict):
                    logger.warning(f"Invalid proceeding data structure: {proceeding}")
                    continue
                
                # Extract data from the proceeding object using the new field mappings
                document_id = proceeding.get('document_id', '')
                document_name = proceeding.get('document_name', '')  # Title
                document_type_id = proceeding.get('document_type_id', '')
                res3 = proceeding.get('res3', '')  # Proceeding Year
                res4 = proceeding.get('res4', '')  # Volume
                res5 = proceeding.get('res5', '')  # No.
                res6 = proceeding.get('res6', '')  # Period
                res7 = proceeding.get('res7', '')  # filename
                res8 = proceeding.get('res8', '')  # Dates
                
                # Skip if no filename (res7)
                if not res7 or res7 == '#' or 'No PDF Found' in res7:
                    continue
                
                # Construct the document URL using the new format
                pdf_url = f"{BASE_URL}/Elibrary_VirtualPath/{document_type_id}/{res7}"
                
                # Generate filename from the new field mappings
                filename = self.generate_filename_new(document_name, res3, res4, res5, res6, res8, res7)
                if not filename:
                    continue
                
                # Create document entry
                document = {
                    'house': house_type.title(),
                    'document_id': document_id,
                    'document_type_id': document_type_id,
                    'title': document_name,
                    'proceeding_year': res3,
                    'volume': res4,
                    'number': res5,
                    'period': res6,
                    'dates': res8,
                    'filename': filename,
                    'url': pdf_url,
                    'identifier': f"{house_type.lower()}.{document_id}.{filename.replace('.pdf', '')}"
                }
                documents.append(document)
            
            logger.info(f"Found {len(documents)} {house_type} documents in proceedings JSON")
            return documents
            
        except Exception as e:
            logger.error(f"Error parsing proceedings JSON from {json_filepath}: {e}")
            return []
    
    def generate_filename_new(self, document_name, proceeding_year, volume, number, period, dates, res7):
        """Generate filename from the new field mappings"""
        try:
            # Use the original filename from res7 as base, but clean it up
            if res7 and res7.endswith('.pdf'):
                base_filename = res7.replace('.pdf', '')
            else:
                base_filename = res7 if res7 else "unknown"
            
            # Clean the base filename
            clean_base = re.sub(r'[^\w\-_.]', '_', base_filename)
            
            # Add proceeding year if available
            year_suffix = ""
            if proceeding_year:
                # Extract year from proceeding_year (might contain HTML breaks)
                year_match = re.search(r'(\d{4})', proceeding_year.replace('<br/>', ' ').replace('<br>', ' '))
                if year_match:
                    year_suffix = f"_{year_match.group(1)}"
            
            # Add volume if available
            volume_suffix = ""
            if volume:
                # Clean volume (might contain HTML breaks)
                clean_volume = re.sub(r'<br/?>', '_', volume)
                clean_volume = re.sub(r'[^\w\-_.]', '_', clean_volume)
                if clean_volume:
                    volume_suffix = f"_vol_{clean_volume}"
            
            # Add number if available
            number_suffix = ""
            if number:
                # Clean number (might contain HTML breaks)
                clean_number = re.sub(r'<br/?>', '_', number)
                clean_number = re.sub(r'[^\w\-_.]', '_', clean_number)
                if clean_number:
                    number_suffix = f"_no_{clean_number}"
            
            # Construct final filename
            filename_parts = [clean_base]
            if year_suffix:
                filename_parts.append(year_suffix)
            if volume_suffix:
                filename_parts.append(volume_suffix)
            if number_suffix:
                filename_parts.append(number_suffix)
            
            filename = "_".join(filename_parts) + ".pdf"
            
            # Ensure filename is not too long
            if len(filename) > 200:
                filename = clean_base + year_suffix + ".pdf"
            
            return filename
            
        except Exception as e:
            logger.warning(f"Error generating filename: {e}")
            return f"proceedings_{int(time.time())}.pdf"

    def generate_filename(self, title, date, day):
        """Generate filename from title, date, and day information (legacy method)"""
        try:
            # Try to extract date from date string
            if date:
                # Look for date patterns like DD-MM-YYYY or DD/MM/YYYY
                date_match = re.search(r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})', date)
                if date_match:
                    day_num, month, year = date_match.groups()
                    date_str = f"{year}_{month}_{day_num}"
                else:
                    # Fallback to using the date as-is
                    date_str = date.replace('/', '_').replace('-', '_').replace(' ', '_')
            else:
                date_str = "unknown_date"
            
            # Extract day number if available
            day_num = ""
            if day:
                day_match = re.search(r'(\d+)', day)
                if day_match:
                    day_num = f"_day_{day_match.group(1)}"
            
            # Clean title for filename
            clean_title = ""
            if title:
                clean_title = re.sub(r'[^\w\s-]', '', title)
                clean_title = re.sub(r'\s+', '_', clean_title.strip())
                clean_title = clean_title[:50]  # Limit length
                if clean_title:
                    clean_title = f"_{clean_title}"
            
            filename = f"proceedings_{date_str}{day_num}{clean_title}.pdf"
            return filename
            
        except Exception as e:
            logger.warning(f"Error generating filename: {e}")
            return f"proceedings_{int(time.time())}.pdf"
    
    
    def download_document(self, document):
        """Download a legislative document PDF"""
        filename = document['filename']
        filepath = self.document_dir / filename
        
        # Check if already exists
        if filepath.exists():
            logger.info(f"Document already exists: {filename}")
            return filepath
        
        try:
            url = document['url']
            logger.info(f"Downloading document: {filename} from {url}")
            
            response = self.session.get(url, timeout=60, stream=True)
            response.raise_for_status()
            
            # Check if response is actually a PDF
            content_type = response.headers.get('content-type', '')
            if 'application/pdf' not in content_type:
                logger.warning(f"Response is not a PDF for {filename}, content-type: {content_type}")
                return None
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded document: {filename}")
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return filepath
            
        except Exception as e:
            logger.error(f"Error downloading document {filename}: {e}")
            return None
    
    def save_document_metadata(self, document):
        """Save document metadata to JSON file"""
        filename = f"{document['filename'].replace('.pdf', '').replace(' ', '_')}.json"
        filepath = self.metadata_dir / filename
        
        try:
            # Prepare metadata with processing timestamp
            metadata = {
                **document,
                'processing_date': datetime.now().isoformat(),
                'source_url': document['url']
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
            identifier = f"westbengallegislature.{document['identifier']}"
            
            # Prepare metadata for Internet Archive
            ia_metadata = {
                'creator': 'West Bengal State Legislature',
                'source': document['url'],
                'mediatype': 'texts',
                'language': ['English', 'Bengali'],
                'licenseurl': 'http://creativecommons.org/licenses/publicdomain/',
                'subject': ['West Bengal State Legislature'],
                'collection': 'parliamentofindia'
            }
            
            # Add extracted metadata
            if metadata_path and metadata_path.exists():
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    doc_metadata = json.load(f)

                if document.get('proceeding_year'):
                    year = document.get('proceeding_year')
                else:
                    year = ""
                if document.get('number'):
                    number = document.get('number')
                else:
                    number = ""
                if document.get('volume'):
                    volume = document.get('volume')
                else:
                    volume = ""
                ia_metadata['title'] = f"{document.get('title')} {year} {volume} {number}"
                ia_metadata['description'] = f"West Bengal State Legislature - {document.get('title')} {year} {volume} {number}"
                ia_metadata['date'] = document['proceeding_year']
                
                # Add specific fields from document metadata
                for key, value in doc_metadata.items():
                    if key not in ['url', 'source_url', 'processing_date', 'identifier', 'filename']:
                        ia_metadata[f"westbengal_legislature_{key}"] = value

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
        metadata_filename = f"{document['filename'].replace('.pdf', '').replace(' ', '_')}.json"
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
            # Create the expected identifier format
            identifier = f"westbengallegislature.{document['identifier']}"
            
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
                        logger.warning(f"Failed to download PDF for {identifier}")
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
                logger.warning(f"Failed to download PDF for {identifier}")
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
    
    def process_all_documents(self):
        """Process all documents from the proceedings JSON APIs"""
        logger.info("Starting to process all West Bengal Legislature documents")
        
        all_documents = []
        
        # Step 0: Load change proceedings page to get cookies
        if not self.load_change_proceedings_page():
            logger.error("Failed to load change proceedings page, cannot proceed")
            return False
        
        # Step 1: Fetch Assembly proceedings
        assembly_path = self.fetch_proceedings_data("assembly")
        if assembly_path:
            assembly_documents = self.parse_proceedings_json(assembly_path, "assembly")
            all_documents.extend(assembly_documents)
            logger.info(f"Found {len(assembly_documents)} Assembly documents")
        
        # Step 2: Fetch Council proceedings
        council_path = self.fetch_proceedings_data("council")
        if council_path:
            council_documents = self.parse_proceedings_json(council_path, "council")
            all_documents.extend(council_documents)
            logger.info(f"Found {len(council_documents)} Council documents")
        
        if not all_documents:
            logger.error("No documents found in proceedings data")
            return False
        
        logger.info(f"Found {len(all_documents)} West Bengal documents to process")
        
        # Step 3: Process each document
        success_count = 0
        total_count = len(all_documents)
        
        for i, document in enumerate(all_documents, 1):
            logger.info(f"Processing document {i}/{total_count}: {document['identifier']}")
            
            if self.process_document(document):
                success_count += 1
            
            # Save progress periodically
            if i % 10 == 0:
                self.save_processed_docs()
                logger.info(f"Progress: {i}/{total_count} documents processed, {success_count} successful")
        
        logger.info(f"Processing complete: {success_count}/{total_count} documents successfully processed")
        return success_count > 0
    
    def run(self):
        """Main execution function"""
        logger.info("Starting West Bengal Legislature Mirror Tool")
        
        try:
            # Process all documents from the proceedings JSON APIs
            success = self.process_all_documents()
            
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
    mirror = WestBengalLegislatureMirror()
    mirror.run()


if __name__ == "__main__":
    main()
