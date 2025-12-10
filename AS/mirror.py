#!/usr/bin/env python3
"""
Assam Legislative Assembly Digital Library Mirror Tool

This script archives documents from the Assam Legislative Assembly Digital Library to:
1. Get the list of collections from http://aladigitallibrary.in/handle/123456789/29
2. In each collection page, get the document corresponding to each day's proceedings in the form of the PDF file for the day
3. Extract the required metadata from the HTML pages and the PDF filename
4. Upload the PDF with the metadata to Internet Archive
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
from bs4 import BeautifulSoup

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
SLEEP_BETWEEN_REQUESTS = 2.0  # seconds
BASE_URL = "http://aladigitallibrary.in"
COLLECTIONS_URL = f"{BASE_URL}/handle/123456789/29"

class AssamLegislatureMirror:
    def __init__(self):
        self.session = requests.Session()
        # Disable SSL verification
        self.session.verify = False
        # Add headers for Assam Legislative Assembly
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
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
        """Search Internet Archive for existing Assam Legislature documents"""
        try:
            logger.info("Searching Internet Archive for existing documents...")
            
            # Search for documents with our creator
            search_query = 'creator:"Assam Legislative Assembly"'
            search_results = ia.search_items(search_query)
            
            existing_count = 0
            for item in search_results:
                identifier = item.get('identifier', '')
                
                # Parse identifier to extract document info
                # Expected format: assamlegislature.{year}.{date}
                if identifier.startswith('assamlegislature.'):
                    # Mark as processed to avoid re-uploading
                    self.processed_docs.add(identifier)
                    existing_count += 1
            
            logger.info(f"Found {existing_count} existing documents on Internet Archive")
            
        except Exception as e:
            logger.warning(f"Error searching Internet Archive: {e}")
            logger.info("Continuing without Internet Archive search results")
    
    def get_collections_list(self):
        """Get the list of collections from the main page"""
        try:
            logger.info(f"Fetching collections list from {COLLECTIONS_URL}")
            
            response = self.session.get(COLLECTIONS_URL, timeout=60)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            collections = []
            
            # Find all collection links
            # Look for links that contain "Assam Legislative Assembly Debates-" followed by a year
            collection_links = soup.find_all('a', href=True)
            
            for link in collection_links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                # Check if this is a collection link
                if 'Assam Legislative Assembly Debates-' in text and href.startswith('/handle/'):
                    # Extract year from the text
                    year_match = re.search(r'Debates-(\d{4})', text)
                    if year_match:
                        year = year_match.group(1)
                        collection_url = urljoin(BASE_URL, href)
                        
                        collection = {
                            'title': text,
                            'year': year,
                            'url': collection_url,
                            'handle': href
                        }
                        collections.append(collection)
                        logger.info(f"Found collection: {text} ({year}) - {collection_url}")
            
            # Sort collections by year
            collections.sort(key=lambda x: int(x['year']))
            
            logger.info(f"Found {len(collections)} collections")
            return collections
            
        except Exception as e:
            logger.error(f"Error fetching collections list: {e}")
            return []
    
    def get_documents_from_collection(self, collection):
        """Get documents from a specific collection"""
        try:
            logger.info(f"Fetching documents from collection: {collection['title']}")
            
            response = self.session.get(collection['url'], timeout=60)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            documents = []
            
            # Find the table with collection items
            # Look for table rows that contain item links
            table_rows = soup.find_all('tr')
            
            for row in table_rows:
                # Look for links in the title column
                title_links = row.find_all('a', href=True)
                
                for link in title_links:
                    href = link.get('href', '')
                    text = link.get_text(strip=True)
                    
                    # Check if this is an item link (contains /handle/)
                    if '/handle/' in href and text:
                        item_url = urljoin(BASE_URL, href)
                        logger.info(f"Found item: {text} - {item_url}")
                        
                        # Get PDFs from this item page
                        item_documents = self.get_pdfs_from_item(item_url, text, collection)
                        documents.extend(item_documents)
            
            logger.info(f"Found {len(documents)} documents in collection {collection['title']}")
            return documents
            
        except Exception as e:
            logger.error(f"Error fetching documents from collection {collection['title']}: {e}")
            return []
    
    def get_pdfs_from_item(self, item_url, item_title, collection):
        """Get PDF files from an individual item page"""
        try:
            logger.info(f"Fetching PDFs from item: {item_title}")
            
            response = self.session.get(item_url, timeout=60)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            documents = []
            
            # Find all links on the item page
            links = soup.find_all('a', href=True)
            
            for link in links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                # Skip links with title "View/Open"
                if text == "View/Open":
                    continue
                
                # Check if this is a PDF link
                if href.endswith('.pdf') or 'pdf' in href.lower():
                    pdf_url = urljoin(BASE_URL, href)
                    
                    # Extract date from filename or item title
                    date_info = self.extract_date_from_filename(text or item_title, collection['year'])
                    
                    if date_info:
                        identifier = f"assamlegislature.{date_info['date']}"
                        
                        document = {
                            'title': f"{date_info['date']}",
                            'filename': f"{date_info['date']}.pdf",
                            'url': pdf_url,
                            'collection': collection,
                            'item_title': item_title,
                            'item_url': item_url,
                            'date': date_info['date'],
                            'date_formatted': date_info['formatted_date'],
                            'identifier': identifier,
                            'fallback': date_info.get('fallback', False)
                        }
                        documents.append(document)
                        logger.info(f"Found PDF: {date_info['date']}")
            
            logger.info(f"Found {len(documents)} PDFs in item: {item_title}")
            return documents
            
        except Exception as e:
            logger.error(f"Error fetching PDFs from item {item_title}: {e}")
            return []
    
    def extract_date_from_filename(self, filename, year):
        """Extract date information from PDF filename in format: Day_Month_Year"""
        try:
            # Month mapping for both full and abbreviated forms
            month_map = {
                'january': '01', 'jan': '01',
                'february': '02', 'feb': '02',
                'march': '03', 'mar': '03',
                'april': '04', 'apr': '04',
                'may': '05',
                'june': '06', 'jun': '06',
                'july': '07', 'jul': '07',
                'august': '08', 'aug': '08',
                'september': '09', 'sep': '09', 'sept': '09',
                'october': '10', 'oct': '10',
                'november': '11', 'nov': '11',
                'december': '12', 'dec': '12'
            }
            
            # Full month names for display
            full_month_map = {
                'jan': 'January', 'feb': 'February', 'mar': 'March', 'apr': 'April',
                'may': 'May', 'jun': 'June', 'jul': 'July', 'aug': 'August',
                'sep': 'September', 'sept': 'September', 'oct': 'October',
                'nov': 'November', 'dec': 'December'
            }
            
            # Pattern to match: Day_Month_Year (with optional ordinal suffixes and various separators)
            patterns = [
                r'(\d{1,2})(?:st|nd|rd|th)?[_\-\s]+(\w+)[_\-\s]+(\d{4})',  # 16th_Month_Year or 16_Month_Year
                r'ALA_Debates_The_(\d{1,2})(?:st|nd|rd|th)?[_\-\s]+(\w+)[_\-\s]+(\d{4})',  # ALA_Debates_The_16th_Month_Year
            ]
            
            for pattern in patterns:
                match = re.search(pattern, filename, re.IGNORECASE)
                if match:
                    day_str, month_str, year_str = match.groups()
                    
                    # Clean up day (remove any non-digit characters)
                    day = re.sub(r'\D', '', day_str)
                    if not day:
                        continue
                    
                    # Convert month to number
                    month_num = month_map.get(month_str.lower())
                    if not month_num:
                        logger.warning(f"Unknown month '{month_str}' in filename: {filename}")
                        continue
                    
                    # Use the year from the collection if it matches, otherwise use the extracted year
                    actual_year = year if year_str == year else year_str
                    
                    # Create date string
                    date_str = f"{actual_year}-{month_num}-{day.zfill(2)}"
                    
                    # Create formatted date for display
                    display_month = full_month_map.get(month_str.lower(), month_str.title())
                    formatted_date = f"{day} {display_month} {actual_year}"
                    
                    return {
                        'date': date_str,
                        'formatted_date': formatted_date,
                        'day': day,
                        'month': display_month,
                        'year': actual_year
                    }
            
            # If no pattern matches, try to extract just the year and create a generic date
            year_match = re.search(r'(\d{4})', filename)
            if year_match and year_match.group(1) == year:
                return {
                    'date': f"{year}-01-01",
                    'formatted_date': f"1 January {year}",
                    'day': '01',
                    'month': 'January',
                    'year': year
                }
            
            # If still no match, create a fallback identifier
            safe_filename = re.sub(r'[^\w\-_.]', '_', filename)
            return {
                'date': f"{year}-01-01",
                'formatted_date': f"1 January {year}",
                'day': '01',
                'month': 'January',
                'year': year,
                'fallback': True,
                'safe_filename': safe_filename
            }
            
        except Exception as e:
            logger.error(f"Error extracting date from filename {filename}: {e}")
            return None
    
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
            
            # Use a separate session for PDF downloads
            pdf_session = requests.Session()
            pdf_session.verify = False
            pdf_session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': document['collection']['url'],
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
                'title': document['title'],
                'filename': document['filename'],
                'url': document['url'],
                'collection': document['collection'],
                'date': document['date'],
                'date_formatted': document['date_formatted'],
                'identifier': document['identifier'],
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
            identifier = document['identifier']
            
            # Prepare metadata for Internet Archive
            ia_metadata = {
                'creator': 'Assam Legislative Assembly',
                'source': document['url'],
                'mediatype': 'texts',
                'language': ['English', 'Assamese'],
                'licenseurl': 'http://creativecommons.org/licenses/publicdomain/',
                'subject': ['Assam Legislative Assembly'],
                'collection': 'parliamentofindia'
            }
            
            # Add extracted metadata
            if metadata_path and metadata_path.exists():
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    doc_metadata = json.load(f)

                ia_metadata['title'] = f"{document['date']}"
                ia_metadata['description'] = f"Assam Legislative Assembly proceedings for {document['date']}"
                
                # Set date
                ia_metadata['date'] = document['date']
                
                # Add specific fields from document metadata
                for key, value in doc_metadata.items():
                    if key not in ['url', 'source_url', 'processing_date', 'filename', 'identifier', 'date', 'date_formatted', 'title', 'collection']:
                        ia_metadata[f"assam_legislature_{key}"] = str(value)

                ia_metadata[f"assam_legislature_collection"] = document['collection']['title']
                ia_metadata[f"assam_legislature_collection_url"] = document['collection']['url']

            # Convert lists to strings joined with commas, except for Language and Subject
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
    
    def process_all_documents(self):
        """Process all documents from all collections"""
        logger.info("Starting to process all Assam Legislature documents")
        
        # Step 1: Get collections list
        collections = self.get_collections_list()
        if not collections:
            logger.error("Failed to fetch collections list")
            return False
        
        logger.info(f"Found {len(collections)} collections to process")
        
        # Step 2: Process each collection
        total_documents = 0
        success_count = 0
        
        for i, collection in enumerate(collections, 1):
            logger.info(f"Processing collection {i}/{len(collections)}: {collection['title']}")
            
            # Get documents from this collection
            documents = self.get_documents_from_collection(collection)
            if not documents:
                logger.warning(f"No documents found in collection: {collection['title']}")
                continue
            
            total_documents += len(documents)
            logger.info(f"Found {len(documents)} documents in collection {collection['title']}")
            
            # Process each document in the collection
            for j, document in enumerate(documents, 1):
                logger.info(f"Processing document {j}/{len(documents)} in collection {collection['title']}: {document['identifier']}")
                
                if self.process_document(document):
                    success_count += 1
                
                # Save progress periodically
                if j % 5 == 0:
                    self.save_processed_docs()
                    logger.info(f"Progress: {j}/{len(documents)} documents processed in collection {collection['title']}")
            
            # Sleep between collections
            time.sleep(SLEEP_BETWEEN_REQUESTS)
        
        logger.info(f"Processing complete: {success_count}/{total_documents} documents successfully processed")
        return success_count > 0
    
    def run(self):
        """Main execution function"""
        logger.info("Starting Assam Legislative Assembly Digital Library Mirror Tool")
        
        try:
            # Process all documents from all collections
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
    mirror = AssamLegislatureMirror()
    mirror.run()


if __name__ == "__main__":
    main()