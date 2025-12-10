#!/usr/bin/env python3
"""
Kerala Legislative Assembly Mirror Tool

This script archives documents from the Kerala Legislative Assembly by:
1. Fetching assembly options from the advanced search page
2. Iterating through each assembly option and all pages
3. Extracting document metadata and IDs
4. Downloading member lists for each document
5. Downloading document PDFs
6. Saving all data in structured directories
"""

import re
import json
import time
import logging
import requests
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlencode
from bs4 import BeautifulSoup
import internetarchive as ia
import shlex

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
BASE_URL = "http://klaproceedings.niyamasabha.org/digital"
ADVANCED_SEARCH_URL = f"{BASE_URL}/index.php?pg=advanced_search_combo"
SEARCH_RESULT_URL = f"{BASE_URL}/adv_search_result.php"
MEMBER_LIST_URL = f"{BASE_URL}/ListSearchMembers.php"
DOCUMENT_PDF_URL = f"{BASE_URL}/docs_to_pdf.php"

class KLAMirror:
    def __init__(self):
        self.session = requests.Session()
        # Add basic headers to mimic browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:140.0) Gecko/20100101 Firefox/140.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        
        # Create output directories
        self.output_dir = Path('raw')
        self.list_dir = self.output_dir / 'list'
        self.metadata_dir = self.output_dir / 'metadata'
        self.documents_dir = Path('documents')
        
        for directory in [self.output_dir, self.list_dir, self.metadata_dir, self.documents_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        # Keep track of processed documents
        self.processed_file = Path('processed_documents.json')
        self.processed_docs = self.load_processed_docs()
        
        # Keep track of successfully uploaded documents
        self.uploaded_file = Path('uploaded_documents.json')
        self.uploaded_docs = self.load_uploaded_docs()
        
        # Keep track of processed assemblies/pages
        self.processed_assemblies_file = Path('processed_assemblies.json')
        self.processed_assemblies = self.load_processed_assemblies()
        
        # Store form token and session info
        self.form_token = None
        self.session_cookie = None
        
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
    
    def load_uploaded_docs(self):
        """Load list of successfully uploaded documents"""
        if self.uploaded_file.exists():
            try:
                with open(self.uploaded_file, 'r') as f:
                    return set(json.load(f))
            except (json.JSONDecodeError, FileNotFoundError):
                return set()
        return set()
    
    def save_uploaded_docs(self):
        """Save list of successfully uploaded documents"""
        with open(self.uploaded_file, 'w') as f:
            json.dump(list(self.uploaded_docs), f, indent=2)
    
    def load_processed_assemblies(self):
        """Load list of already processed assembly/page combinations"""
        if self.processed_assemblies_file.exists():
            try:
                with open(self.processed_assemblies_file, 'r') as f:
                    return set(json.load(f))
            except (json.JSONDecodeError, FileNotFoundError):
                return set()
        return set()
    
    def save_processed_assemblies(self):
        """Save list of processed assembly/page combinations"""
        with open(self.processed_assemblies_file, 'w') as f:
            json.dump(list(self.processed_assemblies), f, indent=2)
    
    def search_existing_ia_documents(self):
        """Search Internet Archive for existing Kerala Legislative Assembly documents"""
        try:
            # Search for documents with our creator
            search_query = 'creator:"Kerala Legislative Assembly"'
            logger.info(f"Searching Internet Archive with query: {search_query}")
            search_results = ia.search_items(search_query)
            
            existing_count = 0
            for item in search_results:
                identifier = item.get('identifier', '')
                
                # Parse identifier to extract document info
                # Expected format: keralalegislativeassembly.proceedings.{doc_id}
                if identifier.startswith('keralalegislativeassembly.proceedings.'):
                    doc_id = identifier.split('.')[-1]
                    if doc_id.isdigit():
                        self.processed_docs.add(doc_id)
                        self.uploaded_docs.add(doc_id)
                        existing_count += 1
            
            logger.info(f"Found {existing_count} existing documents on Internet Archive")
            
        except Exception as e:
            logger.warning(f"Error searching Internet Archive: {e}")
            logger.info("Continuing without Internet Archive search results")
    
    def get_assembly_options(self):
        """Parse assembly options from local assembly.html file"""
        try:
            logger.info("Loading assembly options from assembly.html")
            
            # Read the local assembly.html file
            assembly_file = Path('assembly.html')
            if not assembly_file.exists():
                logger.error("assembly.html file not found")
                return []
            
            with open(assembly_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract options (skip value="0")
            options = []
            for option in soup.find_all('option'):
                value = option.get('value', '')
                text = option.get_text(strip=True)
                if value and value != '0':
                    options.append({'value': value, 'text': text})
            
            logger.info(f"Loaded {len(options)} assembly options from assembly.html")
            return options
            
        except Exception as e:
            logger.error(f"Error loading assembly options from assembly.html: {e}")
            return []
    
    def fetch_search_results(self, assembly_value, page_num):
        """Fetch search results for a specific assembly and page"""
        assembly_page_key = f"{assembly_value}_{page_num}"
        filename = f"{assembly_page_key}.html"
        filepath = self.list_dir / filename
        
        # Skip if already exists
        if filepath.exists():
            logger.info(f"Search results already exist: {filename}")
            return filepath
        
        try:
            logger.info(f"Fetching search results for assembly {assembly_value}, page {page_num}")
            
            # Prepare POST data exactly as shown in curl command
            data = {
                'FlagPost': '1',
                'assembly': assembly_value,
                'session': '',
                'date_search': '',
                'date_search1': '',
                'class_search': '',
                'member': '',
                'subject': '',
                'lang': 'eng',
                'curpage': str(page_num),
            }
            
            # Add form token if available
            if self.form_token:
                data['form_token'] = self.form_token
            
            # Set headers exactly as shown in curl command
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:140.0) Gecko/20100101 Firefox/140.0',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Content-Type': 'application/x-www-form-urlencoded',
                'X-Requested-With': 'XMLHttpRequest',
                'Origin': 'http://klaproceedings.niyamasabha.org',
                'Connection': 'keep-alive',
                'Referer': 'http://klaproceedings.niyamasabha.org/index.php?pg=advanced_search_combo',
                'Priority': 'u=0'
            }
            
            # Convert data to proper URL-encoded format (standard & separator)
            form_data = urlencode(data)
            
            response = self.session.post(
                SEARCH_RESULT_URL,
                data=form_data,
                headers=headers,
                timeout=10
            )
            response.raise_for_status()
            
            # Save to file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            logger.info(f"Saved search results: {filename}")
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return filepath
            
        except Exception as e:
            logger.error(f"Error fetching search results for assembly {assembly_value}, page {page_num}: {e}")
            return None
    
    def extract_documents_from_html(self, html_file):
        """Extract document metadata from HTML search results"""
        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            soup = BeautifulSoup(content, 'html.parser')
            documents = []
            
            # Find all table rows with document data
            results_table = soup.find('div', {'id': 'results'})
            if not results_table:
                logger.warning(f"No results table found in {html_file}")
                return documents
            
            # Find all table rows (skip header row)
            rows = results_table.find_all('tr')[1:]  # Skip header row
            
            for row in rows:
                try:
                    cells = row.find_all('td')
                    if len(cells) < 6:
                        continue
                    
                    # Extract date
                    date = cells[0].get_text(strip=True)
                    
                    # Extract assembly
                    assembly = cells[1].get_text(strip=True)
                    
                    # Extract session
                    session = cells[2].get_text(strip=True)
                    
                    # Extract event (English and Malayalam)
                    event_cell = cells[3]
                    event_eng_div = event_cell.find('div', {'style': lambda x: x and 'display:block' in x})
                    event_mal_div = event_cell.find('div', {'style': lambda x: x and 'display:none' in x})
                    
                    event_eng = event_eng_div.get_text(strip=True) if event_eng_div else ''
                    event_mal = event_mal_div.get_text(strip=True) if event_mal_div else ''
                    
                    # Extract subject (English and Malayalam)
                    subject_cell = cells[4]
                    subject_eng_div = subject_cell.find('div', {'style': lambda x: x and 'display:block' in x})
                    subject_mal_div = subject_cell.find('div', {'style': lambda x: x and 'display:none' in x})
                    
                    subject_eng = subject_eng_div.get_text(strip=True) if subject_eng_div else ''
                    subject_mal = subject_mal_div.get_text(strip=True) if subject_mal_div else ''
                    
                    # Extract document ID from various onclick handlers
                    doc_id = None
                    actions_cell = cells[5]
                    
                    # Try to find document ID from onclick handlers
                    onclick_patterns = [
                        r"OpenClick2\('(\d+)'\)",
                        r"OpenClick1\('(\d+)'\)",
                        r"showhidelang\((\d+)\)",
                        r"memberList=(\d+)"
                    ]
                    
                    for pattern in onclick_patterns:
                        matches = re.findall(pattern, str(actions_cell))
                        if matches:
                            doc_id = matches[0]
                            break
                    
                    if not doc_id:
                        continue
                    
                    # Check if document has PDF available
                    has_pdf = 'docs_to_pdf.php' in str(actions_cell) and 'PDF not found!' not in str(actions_cell)
                    
                    # Check if document has members available
                    has_members = 'OpenClick2' in str(actions_cell) and 'Members not found' not in str(actions_cell)
                    
                    document = {
                        'id': doc_id,
                        'date': date,
                        'assembly': assembly,
                        'session': session,
                        'event_eng': event_eng,
                        'event_mal': event_mal,
                        'subject_eng': subject_eng,
                        'subject_mal': subject_mal,
                        'has_pdf': has_pdf,
                        'has_members': has_members
                    }
                    
                    documents.append(document)
                    
                except Exception as e:
                    logger.warning(f"Error extracting document from row: {e}")
                    continue
            
            logger.info(f"Extracted {len(documents)} documents from {html_file}")
            return documents
            
        except Exception as e:
            logger.error(f"Error extracting documents from {html_file}: {e}")
            return []
    
    def get_member_list(self, doc_id):
        """Fetch member list for a document"""
        try:
            url = f"{MEMBER_LIST_URL}?memberList={doc_id}"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
        
            logger.info(f"Fetched member list for document: {doc_id}")
            
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return response.text
            
        except Exception as e:
            logger.error(f"Error fetching member list for document {doc_id}: {e}")
            return None
    
    def download_document_pdf(self, doc_id):
        """Download document PDF"""
        filename = f"{doc_id}.pdf"
        filepath = self.documents_dir / filename
        
        # Skip if already exists
        if filepath.exists():
            logger.info(f"Document PDF already exists: {filename}")
            return filepath
        
        try:
            url = f"{DOCUMENT_PDF_URL}?memberList={doc_id}"
            response = self.session.get(url, timeout=60, stream=True)
            response.raise_for_status()
            
            # Check if response is actually a PDF
            content_type = response.headers.get('content-type', '')
            if 'application/pdf' not in content_type:
                logger.warning(f"Response is not a PDF for {doc_id}, content-type: {content_type}")
                return None
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded document PDF: {filename}")
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return filepath
            
        except Exception as e:
            logger.error(f"Error downloading document PDF for {doc_id}: {e}")
            return None
    
    def parse_member_list_html(self, member_list_html):
        """Parse member list HTML and extract member names in both English and Malayalam"""
        if not member_list_html:
            return []
        
        try:
            soup = BeautifulSoup(member_list_html, 'html.parser')
            members = []
            
            # Find the table containing member information
            table = soup.find('table')
            if not table:
                logger.warning("No table found in member list HTML")
                return members
            
            # Find all data rows (skip header row)
            rows = table.find_all('tr')
            if len(rows) <= 1:
                logger.warning("No data rows found in member list table")
                return members
            
            # Process each data row (skip header)
            for row in rows[1:]:
                try:
                    cells = row.find_all('td')
                    if len(cells) < 3:
                        continue
                    
                    # Extract member information
                    # cells[0] = No, cells[1] = English Name, cells[2] = Malayalam Name
                    member_no = cells[0].get_text(strip=True)
                    english_name = cells[1].get_text(strip=True)
                    malayalam_name = cells[2].get_text(strip=True)
                    
                    if english_name or malayalam_name:
                        member = {
                            'no': member_no,
                            'english_name': english_name,
                            'malayalam_name': malayalam_name
                        }
                        members.append(member)
                        
                except Exception as e:
                    logger.warning(f"Error parsing member row: {e}")
                    continue
            
            logger.info(f"Parsed {len(members)} members from member list HTML")
            return members
            
        except Exception as e:
            logger.error(f"Error parsing member list HTML: {e}")
            return []

    def save_document_metadata(self, document, member_list_html=None):
        """Save document metadata to JSON file"""
        doc_id = document['id']
        filename = f"{doc_id}.json"
        filepath = self.metadata_dir / filename
        
        try:
            # Parse member list HTML if available
            members = self.parse_member_list_html(member_list_html) if member_list_html else []
            
            # Prepare metadata
            metadata = {
                **document,
                'member_list_url': f"{MEMBER_LIST_URL}?memberList={doc_id}",
                'document_pdf_url': f"{DOCUMENT_PDF_URL}?memberList={doc_id}",
                'members': members
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved document metadata: {filename}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving document metadata for {doc_id}: {e}")
            return None
    
    def upload_to_internet_archive(self, doc_id, metadata_path, pdf_path):
        """Upload document and metadata to Internet Archive"""
        try:
            # Create identifier for Internet Archive
            identifier = f"keralalegislativeassembly.proceedings.{doc_id}"
            
            # Prepare metadata for Internet Archive
            ia_metadata = {
                'creator': 'Kerala Legislative Assembly',
                'source': f'{BASE_URL}',
                'mediatype': 'texts',
                'language': ['English', 'Malayalam'],
                'licenseurl': 'http://creativecommons.org/licenses/publicdomain/',
                'subject': ['Kerala Legislative Assembly', 'Government Documents', 'Kerala'],
                'title': f'{doc_id}',
            }
            
            # Add extracted metadata
            if metadata_path and metadata_path.exists():
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    doc_metadata = json.load(f)
                
                # Update title and description with document info
                if doc_metadata.get('subject_eng'):
                    ia_metadata['title'] = f"{doc_metadata['subject_eng']} / {doc_metadata['subject_mal']}"
                
                description_parts = []
                if doc_metadata.get('date'):
                    description_parts.append(f"Date: {doc_metadata['date']}")
                if doc_metadata.get('assembly'):
                    description_parts.append(f"Assembly: {doc_metadata['assembly']}")
                if doc_metadata.get('session'):
                    description_parts.append(f"Session: {doc_metadata['session']}")
                if doc_metadata.get('event_eng'):
                    description_parts.append(f"Event: {doc_metadata['event_eng']}")
                if doc_metadata.get('subject_eng'):
                    description_parts.append(f"Subject: {doc_metadata['subject_eng']}")
                
                ia_metadata['description'] = 'Kerala Legislative Assembly Proceedings\n' + '\n'.join(description_parts)
                
                # Add date if available
                if doc_metadata.get('date'):
                    # Convert date format from DD-MM-YYYY to YYYY-MM-DD
                    date_parts = doc_metadata['date'].split('-')
                    if len(date_parts) == 3:
                        ia_metadata['date'] = f"{date_parts[2]}-{date_parts[1]}-{date_parts[0]}"
                
                # Add specific fields from document metadata
                for key, value in doc_metadata.items():
                    if key not in ['id', 'member_list_html', 'has_pdf', 'has_members', 'members']:
                        ia_metadata[f"kla_{key}"] = value
                
                # Handle members field specially
                if 'members' in doc_metadata and doc_metadata['members']:
                    # Convert members to a string representation for Internet Archive
                    member_names_eng = []
                    member_names_mal = []
                    for member in doc_metadata['members']:
                        if member.get('english_name'):
                            member_names_eng.append(member['english_name'])
                        if member.get('malayalam_name'):
                            member_names_mal.append(member['malayalam_name'])
                    ia_metadata['kla_members_eng'] = ', '.join(member_names_eng)
                    ia_metadata['kla_members_mal'] = ', '.join(member_names_mal)

                # Make kla_document_pdf_url and kla_member_list_url the last two fields
                del ia_metadata['kla_document_pdf_url']
                del ia_metadata['kla_member_list_url']
                ia_metadata['kla_document_pdf_url'] = doc_metadata['document_pdf_url']
                ia_metadata['kla_member_list_url'] = doc_metadata['member_list_url']
            
            # Convert lists to strings joined with commas, except for certain fields
            for key, value in ia_metadata.items():
                if isinstance(value, list) and key not in ['language', 'subject']:
                    ia_metadata[key] = ', '.join(value)

            identifier = f"keralalegislativeassembly.proceedings.{doc_metadata['id']}"
            
            # Prepare files to upload
            files = []
            
            # Add PDF if available
            if pdf_path and pdf_path.exists():
                files.append(str(pdf_path))
            
            if files:
                logger.info(f"Uploading to Internet Archive: {identifier}")
                logger.info(f"Files: {[f for f in files]}")
                logger.info(f"Metadata: {ia_metadata}")
                
                item = ia.get_item(identifier)
                item.upload(files, metadata=ia_metadata, verbose=True, verify=True, checksum=True, retries=3, retries_sleep=10)
                
                logger.info(f"Successfully uploaded to Internet Archive: {identifier}")
                return True
            else:
                logger.warning(f"No files to upload for document {doc_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error uploading to Internet Archive for document {doc_id}: {e}")
            return False
    
    def process_document(self, document):
        """Process a single document: extract metadata, get member list, download PDF, upload to IA"""
        doc_id = document['id']
        
        # Skip if already uploaded
        if doc_id in self.uploaded_docs:
            logger.info(f"Document already uploaded: {doc_id}")
            return True
        
        logger.info(f"Processing document: {doc_id}")
        
        try:
            # Step 1: Get member list if available
            member_list_html = None
            if document.get('has_members'):
                member_list_html = self.get_member_list(doc_id)
            
            # Step 2: Save metadata
            metadata_path = self.save_document_metadata(document, member_list_html)
            if not metadata_path:
                return False
            
            # Step 3: Download PDF if available
            pdf_path = None
            if document.get('has_pdf'):
                pdf_path = self.download_document_pdf(doc_id)
            
            # Step 4: Upload to Internet Archive
            upload_success = self.upload_to_internet_archive(doc_id, metadata_path, pdf_path)
            
            # Mark as processed and uploaded if successful
            if upload_success:
                self.processed_docs.add(doc_id)
                self.uploaded_docs.add(doc_id)
                self.save_processed_docs()
                self.save_uploaded_docs()
                
                # Clean up local PDF file after successful upload
                if pdf_path and pdf_path.exists():
                    try:
                        pdf_path.unlink()
                        logger.info(f"Deleted local PDF file: {pdf_path}")
                    except Exception as e:
                        logger.warning(f"Failed to delete local PDF file {pdf_path}: {e}")
                
                logger.info(f"Successfully processed and uploaded document: {doc_id}")
            else:
                logger.warning(f"Failed to upload document {doc_id} to Internet Archive")
            
            return upload_success
            
        except Exception as e:
            logger.error(f"Error processing document {doc_id}: {e}")
            return False
    
    def process_assembly_page(self, assembly_value, page_num):
        """Process all documents for a specific assembly and page"""
        assembly_page_key = f"{assembly_value}_{page_num}"
        
        # Skip if already processed
        if assembly_page_key in self.processed_assemblies:
            logger.info(f"Assembly page already processed: {assembly_page_key}")
            return True
        
        # Step 1: Fetch search results
        html_file = self.fetch_search_results(assembly_value, page_num)
        if not html_file:
            return False
        
        # Step 2: Extract documents from HTML
        documents = self.extract_documents_from_html(html_file)
        if not documents:
            logger.info(f"No documents found for assembly {assembly_value}, page {page_num}")
            # Mark as processed even if no documents found
            self.processed_assemblies.add(assembly_page_key)
            self.save_processed_assemblies()
            return True
        
        # Step 3: Process each document
        success_count = 0
        for document in documents:
            if self.process_document(document):
                success_count += 1
        
        logger.info(f"Successfully processed {success_count}/{len(documents)} documents for assembly {assembly_value}, page {page_num}")
        
        # Mark page as processed
        self.processed_assemblies.add(assembly_page_key)
        self.save_processed_assemblies()
        
        return success_count > 0
    
    def has_more_pages(self, html_file):
        """Check if there are more pages based on pagination"""
        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            soup = BeautifulSoup(content, 'html.parser')
            
            # Look for "Next >>" link
            next_links = soup.find_all('a', string=re.compile(r'Next\s*>>'))
            if not next_links:
                return False
            
            # Check if the Next link is actually clickable (has onclick)
            for link in next_links:
                if link.get('onclick'):
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking for more pages in {html_file}: {e}")
            return False
    
    def process_assembly(self, assembly_option):
        """Process all pages for a specific assembly"""
        assembly_value = assembly_option['value']
        assembly_text = assembly_option['text']
        
        logger.info(f"Processing assembly: {assembly_text} (value: {assembly_value})")
        
        page_num = 1
        while True:
            assembly_page_key = f"{assembly_value}_{page_num}"
            
            # Skip if already processed
            if assembly_page_key in self.processed_assemblies:
                logger.info(f"Assembly page already processed: {assembly_page_key}")
                page_num += 1
                continue
            
            # Process this page
            success = self.process_assembly_page(assembly_value, page_num)
            if not success:
                logger.warning(f"Failed to process assembly {assembly_value}, page {page_num}")
                break
            
            # Check if there are more pages
            html_file = self.list_dir / f"{assembly_page_key}.html"
            if not self.has_more_pages(html_file):
                logger.info(f"No more pages for assembly {assembly_value}")
                break
            
            page_num += 1
            
            # Safety check to avoid infinite loops
            if page_num > 1000:
                logger.warning(f"Reached maximum page limit for assembly {assembly_value}")
                break
        
        logger.info(f"Completed processing assembly: {assembly_text}")
    
    def run(self):
        """Main execution function"""
        logger.info("Starting Kerala Legislative Assembly Mirror Tool")
        
        try:
            # Step 1: Get assembly options
            assembly_options = self.get_assembly_options()
            if not assembly_options:
                logger.error("No assembly options found")
                return
            
            # Step 2: Process each assembly
            for assembly_option in assembly_options:
                try:
                    self.process_assembly(assembly_option)
                    
                    # Save progress periodically
                    self.save_processed_assemblies()
                    self.save_processed_docs()
                    self.save_uploaded_docs()
                    
                except Exception as e:
                    logger.error(f"Error processing assembly {assembly_option}: {e}")
                    continue
            
            logger.info("Mirror tool completed")
            
        except KeyboardInterrupt:
            logger.info("Process interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.save_processed_assemblies()
            self.save_processed_docs()
            self.save_uploaded_docs()


def main():
    """Main entry point"""
    mirror = KLAMirror()
    mirror.run()


if __name__ == "__main__":
    main()
