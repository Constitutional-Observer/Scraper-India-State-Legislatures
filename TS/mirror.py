#!/usr/bin/env python3
"""
Telangana Legislature Mirror Tool

This script archives documents from the Telangana Legislature to:
1. Parse the hierarchical archives tree structure (Assembly/Council > Terms > Sessions > Sittings)
2. Extract document metadata and download URLs
3. Download legislative debate PDF documents
4. Upload to Internet Archive with proper metadata
5. Filter to include only Telangana-specific documents, excluding Hyderabad and Andhra Pradesh
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
BASE_URL = "https://legislature.telangana.gov.in"
ARCHIVES_URL = f"{BASE_URL}/debates"
DOCUMENT_BASE_URL = "https://sessions-legislature.telangana.gov.in/PreviewPage.do"

class TelanganaLegislatureMirror:
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
    
    
    def search_existing_ia_documents(self):
        """Search Internet Archive for existing Telangana Legislature documents"""
        try:
            logger.info("Searching Internet Archive for existing documents...")
            
            # Search for documents with our creator
            search_query = 'creator:"Telangana State Legislature"'
            search_results = ia.search_items(search_query)
            
            existing_count = 0
            for item in search_results:
                identifier = item.get('identifier', '')
                
                # Parse identifier to extract document info
                # Expected format: telanganalegislature.{house}.{term}.{session}.{sitting}.{filename}
                if identifier.startswith('telanganalegislature.'):
                    # Mark as processed to avoid re-uploading
                    self.processed_docs.add(identifier)
                    existing_count += 1
            
            logger.info(f"Found {existing_count} existing documents on Internet Archive")
            
        except Exception as e:
            logger.warning(f"Error searching Internet Archive: {e}")
            logger.info("Continuing without Internet Archive search results")
    
    def fetch_archives_tree(self):
        """Fetch the main archives tree page"""
        filename = "archives_tree.html"
        filepath = self.tree_dir / filename
        
        try:
            logger.info("Fetching archives tree page...")
            
            response = self.session.get(ARCHIVES_URL, timeout=30)
            response.raise_for_status()
            
            # Save HTML to file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            logger.info(f"Saved archives tree: {filename}")
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return filepath
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching archives tree: {e}")
            return None
    
    def parse_archives_tree(self, tree_filepath):
        """Parse the archives tree HTML to extract document structure and URLs"""
        try:
            with open(tree_filepath, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            soup = BeautifulSoup(html_content, 'html.parser')
            documents = []
            
            # Find the main tree structure
            main_tree = soup.find('ul', class_='tree')
            if not main_tree:
                logger.error("No main tree found in archives page")
                return []
            
            # Parse Assembly and Council sections
            houses = main_tree.find_all('li', recursive=False)
            for house_li in houses:
                house_span = house_li.find('span', class_='English toggler')
                if not house_span:
                    continue
                
                house_text = house_span.get_text(strip=True)
                house_name = "Assembly" if "Assembly" in house_text else "Council" if "Council" in house_text else "Unknown"
                
                logger.info(f"Processing {house_name}...")
                
                # Get the house-specific container
                house_container = house_li.find('ul')
                if not house_container:
                    continue
                
                # Parse terms/sessions within each house
                terms = house_container.find_all('li', recursive=False)
                for term_li in terms:
                    term_span = term_li.find('span')
                    if not term_span:
                        continue
                    
                    term_text = term_span.get_text(strip=True)
                    
                    # Skip if this is the old united council (pre-bifurcation)
                    if 'unitedCouncilID' in str(term_li) or 'aplegislature.org' in str(term_li):
                        logger.info(f"Skipping pre-bifurcation {house_name} term: {term_text}")
                        continue
                    
                    # Parse sessions within each term
                    sessions = term_li.find_all('ul', recursive=False)
                    for session_ul in sessions:
                        session_items = session_ul.find_all('li', recursive=False)
                        for session_li in session_items:
                            session_span = session_li.find('span')
                            if not session_span:
                                continue
                            
                            session_text = session_span.get_text(strip=True)
                            
                            # Parse sittings within each session
                            sittings = session_li.find_all('ul', recursive=False)
                            for sitting_ul in sittings:
                                sitting_items = sitting_ul.find_all('li', recursive=False)
                                for sitting_li in sitting_items:
                                    sitting_span = sitting_li.find('span')
                                    if not sitting_span:
                                        continue
                                    
                                    sitting_text = sitting_span.get_text(strip=True)
                                    
                                    # Parse individual document days within each sitting
                                    days = sitting_li.find_all('ul', recursive=False)
                                    for day_ul in days:
                                        day_items = day_ul.find_all('li', recursive=False)
                                        for day_li in day_items:
                                            day_link = day_li.find('a')
                                            if not day_link:
                                                continue
                                            
                                            href = day_link.get('href', '')
                                            day_text = day_link.get_text(strip=True).replace('Day', '').replace('(', '').replace(')', '').replace(' ', '')
                                            
                                            # Skip empty or placeholder links
                                            if not href or href == '#' or 'No PDF Found' in href:
                                                continue
                                            
                                            # Filter out Andhra Pradesh and Hyderabad documents
                                            if self.should_skip_document(href, term_text, session_text, sitting_text, day_text):
                                                logger.info(f"Skipping non-Telangana document: {day_text}")
                                                continue
                                            
                                            # Extract filename from URL or day text
                                            filename = self.extract_filename(href, day_text)
                                            if filename:
                                                document = {
                                                    'house': house_name,
                                                    'term': term_text,
                                                    'session': session_text,
                                                    'sitting': sitting_text,
                                                    'day': day_text,
                                                    'filename': filename.replace(' ', '_').replace('Uploads/', ''),
                                                    'url': href,
                                                    'identifier': f"{house_name.lower()}.{filename.replace('.pdf', '').replace('-', '.').replace(' ', '_').replace('Uploads/', '')}"
                                                }
                                                documents.append(document)
            
            logger.info(f"Found {len(documents)} Telangana documents in archives tree")
            return documents
            
        except Exception as e:
            logger.error(f"Error parsing archives tree from {tree_filepath}: {e}")
            return []
    
    def should_skip_document(self, href, term_text, session_text, sitting_text, day_text):
        """Check if document should be skipped based on Telangana filtering criteria"""
        # Skip documents from Andhra Pradesh legislature
        if 'aplegislature.org' in href:
            return True
        
        # Skip documents that mention Hyderabad in a non-Telangana context
        # (Hyderabad is part of Telangana, but we want to exclude old Hyderabad state documents)
        if 'hyderabad' in term_text.lower() and 'telangana' not in term_text.lower():
            return True
        
        # Skip documents that mention Andhra Pradesh
        if 'andhra pradesh' in term_text.lower() or 'andhra pradesh' in session_text.lower():
            return True
        
        # Skip very old documents that are likely pre-bifurcation
        # Telangana was formed in 2014, so documents before that are likely from united AP
        date_match = re.search(r'(\d{4})', term_text)
        if date_match:
            year = int(date_match.group(1))
            if year < 2014:
                return True
        
        return False
    
    def extract_filename(self, href, day_text):
        """Extract filename from URL or day text"""
        # Try to extract from URL first
        if 'fileName=' in href:
            filename_match = re.search(r'fileName=([^&]+)', href)
            if filename_match:
                return filename_match.group(1)
        
        # Try to extract from base64 encoded URL
        if 'q=' in href:
            try:
                import base64
                from urllib.parse import unquote
                encoded_part = href.split('q=')[1]
                decoded = base64.b64decode(encoded_part).decode('utf-8')
                filename_match = re.search(r'fileName=([^&]+)', decoded)
                if filename_match:
                    return unquote(filename_match.group(1))
            except:
                pass
        
        # Fallback: generate filename from day text
        if day_text:
            # Extract date from day text
            date_match = re.search(r'(\d{2}-\d{2}-\d{4})', day_text)
            if date_match:
                date_str = date_match.group(1).replace('-', '_')
                return f"day_{date_str}.pdf"
            
            # Extract day number
            day_match = re.search(r'day\s*(\d+)', day_text.lower())
            if day_match:
                day_num = day_match.group(1)
                return f"day_{day_num}.pdf"
        
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
            identifier = f"telanganalegislature.{document['identifier']}"
            
            # Prepare metadata for Internet Archive
            ia_metadata = {
                'creator': 'Telangana State Legislature',
                'source': document['url'],
                'mediatype': 'texts',
                'language': ['Telugu', 'English'],
                'licenseurl': 'http://creativecommons.org/licenses/publicdomain/',
                'subject': ['Telangana State Legislature'],
                'collection': 'parliamentofindia'
            }
            
            # Add extracted metadata
            if metadata_path and metadata_path.exists():
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    doc_metadata = json.load(f)

                ia_metadata['title'] = f"{document['house']} ({document['day']})"
                ia_metadata['description'] = f"Telangana State Legislature {document['house']} proceedings - {document['term']}, {document['session']}, {document['sitting']}, Day {document['day']}"
                
                # Extract date from filename if possible
                date_match = re.search(r'(\d{2}-\d{2}-\d{4})', document['filename'])
                if date_match:
                    ia_metadata['date'] = date_match.group(1)
                
                # Add specific fields from document metadata
                for key, value in doc_metadata.items():
                    if key not in ['url', 'source_url', 'processing_date', 'identifier', 'filename']:
                        ia_metadata[f"telangana_legislature_{key}"] = value

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
            identifier = f"telanganalegislature.{document['identifier']}"
            
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
        """Process all documents from the archives tree"""
        logger.info("Starting to process all Telangana Legislature documents")
        
        # Step 1: Fetch archives tree
        tree_path = self.fetch_archives_tree()
        if not tree_path:
            logger.error("Failed to fetch archives tree")
            return False
        
        # Step 2: Parse tree to extract documents
        documents = self.parse_archives_tree(tree_path)
        if not documents:
            logger.error("No documents found in archives tree")
            return False
        
        logger.info(f"Found {len(documents)} Telangana documents to process")
        
        # Step 3: Process each document
        success_count = 0
        total_count = len(documents)
        
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
    
    def run(self):
        """Main execution function"""
        logger.info("Starting Telangana Legislature Mirror Tool")
        
        try:
            # Process all documents from the archives tree
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
    mirror = TelanganaLegislatureMirror()
    mirror.run()


if __name__ == "__main__":
    main()
