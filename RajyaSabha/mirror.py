#!/usr/bin/env python3
"""
rsdebate Mirror Tool

This script archives documents from the RS Debates Digital Library (rsdebate.nic.in) to:
1. Download document detail pages
2. Extract metadata from HTML pages
3. Download PDF documents
4. Upload to Internet Archive
5. Monitor directory size (terminate at 10GB)
"""

import os
import json
import time
import logging
import requests
import urllib3
from pathlib import Path
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
SLEEP_BETWEEN_REQUESTS = 0.5  # seconds
BATCH_SIZE = 10  # Process documents in batches
BASE_URL = "https://rsdebate.nic.in"

class rsdebateMirror:
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
        self.html_dir = self.output_dir / 'htmls'
        self.pdf_dir = self.output_dir / 'pdfs'
        self.metadata_dir = self.output_dir / 'metadata'
        
        for directory in [self.output_dir, self.html_dir, self.pdf_dir, self.metadata_dir]:
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
        """Search Internet Archive for existing RS Debates documents"""
        try:
            logger.info("Searching Internet Archive for existing documents...")
            
            # Search for documents with our creator
            search_query = 'creator:"Rajya Sabha Secretariat"'
            search_results = ia.search_items(search_query)
            
            existing_count = 0
            for item in search_results:
                identifier = item.get('identifier', '')
                
                # Parse identifier to extract doc_id
                # Expected format: rsdebate.nic.in.{doc_id}
                if identifier.startswith('rsdebate.nic.in.'):
                    doc_id = identifier[len('rsdebate.nic.in.'):]
                    self.processed_docs.add(doc_id)
                    existing_count += 1
            
            logger.info(f"Found {existing_count} existing documents on Internet Archive")
            
        except Exception as e:
            logger.warning(f"Error searching Internet Archive: {e}")
            logger.info("Continuing without Internet Archive search results")
    
    def fetch_document_page(self, doc_id):
        """Fetch document page and save to raw/htmls directory"""
        url = f"{BASE_URL}/handle/123456789/{doc_id}"
        filename = f"{doc_id}.html"
        filepath = self.html_dir / filename
        
        # Skip if already exists
        if filepath.exists():
            logger.info(f"Document page already exists: {filename}")
            return filepath
        
        try:
            logger.info(f"Fetching document page: {url}")
            response = self.session.get(url, timeout=30)
            
            # Skip if 404 or other error
            if response.status_code == 404:
                logger.info(f"Document {doc_id} not found")
                return None
                
            response.raise_for_status()
            
            # Skip if not a valid document page
            if 'Appears in Collections' not in response.text:
                logger.info(f"Not a valid document page: {doc_id}")
                return None
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            logger.info(f"Saved document page: {filename}")
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return filepath
            
        except Exception as e:
            logger.error(f"Error fetching document page {url}: {e}")
            return None
    
    def extract_metadata_from_page(self, html_filepath):
        """Extract metadata from document page"""
        try:
            with open(html_filepath, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
            
            metadata = {}
            
            # Extract metadata from table
            table = soup.find('table', class_='itemDisplayTable')
            if table:
                for row in table.find_all('tr'):
                    cells = row.find_all(['td'])
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True).rstrip(':')
                        # Normalize key
                        key = key.lower().replace(' ', '_')
                        value = cells[1].get_text(strip=True)
                        metadata[key] = value
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error extracting metadata from {html_filepath}: {e}")
            return None
    
    def extract_pdf_url_from_page(self, html_filepath):
        """Extract all PDF URLs from document page"""
        try:
            with open(html_filepath, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
            
            pdf_urls = []
            
            # Find PDF links in the files table
            files_table = soup.find('table', class_='panel-body')
            if files_table:
                for row in files_table.find_all('tr')[1:]:  # Skip header row
                    cells = row.find_all('td')
                    if cells and len(cells) >= 5:
                        link = cells[0].find('a')
                        if link and link['href'].endswith('.pdf'):
                            pdf_urls.append(urljoin(BASE_URL, link['href']))
            
            if not pdf_urls:
                logger.warning(f"No PDF links found in {html_filepath}")
            else:
                logger.info(f"Found {len(pdf_urls)} PDF links in {html_filepath}")
                
            return pdf_urls
            
        except Exception as e:
            logger.error(f"Error extracting PDF URLs from {html_filepath}: {e}")
            return []
    
    def download_pdf(self, pdf_url, doc_id, file_index=0):
        """Download PDF document
        
        Args:
            pdf_url: URL of the PDF to download
            doc_id: Document ID
            file_index: Index of the PDF file (0 for single PDFs, >0 for multiple PDFs)
        """
        # Create document-specific directory
        doc_dir = self.pdf_dir / str(doc_id)
        doc_dir.mkdir(exist_ok=True)
        
        # Extract original filename from URL
        url_path = urlparse(pdf_url).path
        original_filename = os.path.basename(url_path)
        if not original_filename.endswith('.pdf'):
            # Fallback to index-based naming if URL doesn't end with .pdf
            original_filename = f"document_{file_index}.pdf"
            
        filepath = doc_dir / original_filename
        
        # Skip if already exists
        if filepath.exists():
            logger.info(f"PDF already exists: {filepath}")
            return filepath
        
        try:
            logger.info(f"Downloading PDF {file_index + 1}: {pdf_url}")
            response = self.session.get(pdf_url, timeout=60, stream=True)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded PDF: {filepath}")
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return filepath
            
        except Exception as e:
            logger.error(f"Error downloading PDF {pdf_url}: {e}")
            return None
    
    def save_metadata(self, metadata, doc_id):
        """Save metadata to JSON file"""
        filename = f"{doc_id}.json"
        filepath = self.metadata_dir / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved metadata: {filename}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving metadata: {e}")
            return None
    
    def upload_to_internet_archive(self, doc_id, metadata_path, pdf_paths, page_path):
        """Upload document and metadata to Internet Archive"""
        try:
            # Create identifier for Internet Archive
            identifier = f"rsdebate.nic.in.{doc_id}"
            
            # Prepare metadata for Internet Archive
            # Get the first PDF URL from the document page
            pdf_urls = self.extract_pdf_url_from_page(page_path)
            source_url = pdf_urls[0] if pdf_urls else f'https://rsdebate.nic.in/handle/123456789/{doc_id}'
            
            ia_metadata = {
                'creator': 'Rajya Sabha Secretariat',
                'source': f'https://rsdebate.nic.in/handle/123456789/{doc_id}',
                'mediatype': 'texts',
                'language': ['English', 'Hindi'],
                'licenseurl': 'http://creativecommons.org/licenses/publicdomain/',
                'subject': ['Parliament of India', 'Rajya Sabha'],
            }
            
            # Add extracted metadata
            if metadata_path and metadata_path.exists():
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    doc_metadata = json.load(f)
                
                # Map fields to Internet Archive metadata
                if 'debate_title' in doc_metadata:
                    ia_metadata['title'] = f'{doc_metadata["debate_title"]} ({doc_metadata["debate_date"]})'
                if 'debate_date' in doc_metadata:
                    ia_metadata['date'] = doc_metadata['debate_date']
                
                # Add all fields as custom metadata
                for key, value in doc_metadata.items():
                    ia_metadata[f"rsdebate_{key.lower()}"] = value

            ia_metadata['description'] = f"'{ia_metadata['title']}' from the RS Debates Digital Library"
            ia_metadata['rsdebate_document_url'] = source_url

            ia_metadata['rsdebate_ministry'] = ia_metadata['rsdebate_minsitry']
            del(ia_metadata['rsdebate_minsitry'])
            ia_metadata['rsdebate_ministers_name'] = ia_metadata['rsdebate_ministers_name_\t']
            del(ia_metadata['rsdebate_ministers_name_\t'])
            
            # Prepare files to upload
            files = []
            if pdf_paths:
                files.extend([str(path) for path in pdf_paths])
            
            if files:
                logger.info(f"Uploading to Internet Archive: {identifier}")
                logger.info(f"Adding metadata: {ia_metadata}")
                logger.info(f"Files: {files}")
                item = ia.get_item(identifier)
                item.upload(files, metadata=ia_metadata, verbose=True, verify=True, checksum=True, retries=3, retries_sleep=10)
                if item:
                    logger.info(f"Successfully uploaded: {identifier}")
                else:
                    logger.info(f"Failed to upload: {identifier}")
                return True
            else:
                logger.warning(f"No files to upload for {identifier}")
                return False
                
        except Exception as e:
            logger.error(f"Error uploading to Internet Archive: {e}")
            return False
    
    def process_document(self, doc_id):
        """Process a single document: fetch page, extract metadata, download PDF, upload to IA"""
        # Skip if already processed
        if str(doc_id) in self.processed_docs:
            logger.info(f"Document already processed: {doc_id}")
            return True
        
        logger.info(f"Processing document: {doc_id}")
        
        # Step 1: Fetch document page
        page_path = self.fetch_document_page(doc_id)
        if not page_path:
            return False
        
        # Step 2: Extract metadata
        metadata = self.extract_metadata_from_page(page_path)
        metadata_path = None
        if metadata:
            metadata_path = self.save_metadata(metadata, doc_id)
        
        # Step 3: Extract PDF URLs and download
        pdf_urls = self.extract_pdf_url_from_page(page_path)
        pdf_paths = []
        for idx, pdf_url in enumerate(pdf_urls):
            pdf_path = self.download_pdf(pdf_url, doc_id, idx)
            if pdf_path:
                pdf_paths.append(pdf_path)
        
        # Step 4: Upload to Internet Archive
        upload_success = self.upload_to_internet_archive(doc_id, metadata_path, pdf_paths, page_path)
        
        # Mark as processed if successful
        if upload_success:
            self.processed_docs.add(str(doc_id))
            self.save_processed_docs()
            logger.info(f"Successfully processed document: {doc_id}")
        
        return upload_success
    
    def get_last_processed_id(self):
        """Get the highest document ID that has been processed"""
        if not self.processed_docs:
            return 0
            
        try:
            # Convert all IDs to integers and find max
            numeric_ids = [int(doc_id) for doc_id in self.processed_docs if doc_id.isdigit()]
            return max(numeric_ids) if numeric_ids else 0
        except Exception as e:
            logger.error(f"Error finding last processed ID: {e}")
            return 0

    def run(self):
        """Main execution function"""
        logger.info("Starting RS Debates Mirror Tool")
        
        try:
            # Start from the last processed document ID
            start_id = self.get_last_processed_id() + 1
            end_id = 30000000
            
            logger.info(f"Starting from document ID: {start_id}")
            
            for doc_id in range(start_id, end_id + 1, BATCH_SIZE):
                logger.info(f"Processing batch starting at {doc_id}")
                
                for current_id in range(doc_id, min(doc_id + BATCH_SIZE, end_id + 1)):
                    success = self.process_document(current_id)
                    
                    # Save progress periodically
                    if current_id % 10 == 0:
                        self.save_processed_docs()
                
                logger.info(f"Completed batch up to {min(doc_id + BATCH_SIZE - 1, end_id)}")

                # Delete all files in the output directory
                for file in self.output_dir.glob('**/*'):
                    if file.is_file():
                        file.unlink()
            
            logger.info("Mirror tool completed")
            
        except KeyboardInterrupt:
            logger.info("Process interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.save_processed_docs()


def main():
    """Main entry point"""
    mirror = rsdebateMirror()
    mirror.run()


if __name__ == "__main__":
    main()
