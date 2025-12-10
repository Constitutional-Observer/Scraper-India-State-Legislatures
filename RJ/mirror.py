#!/usr/bin/env python3
"""
Rajasthan Legislative Assembly Mirror Tool

This script archives documents from the Rajasthan Legislative Assembly to:
1. Parse the house proceedings page structure
2. Extract document metadata and download URLs for all sessions and dates
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
from datetime import datetime, date
from urllib.parse import urlparse, urljoin, parse_qs
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
BASE_URL = "https://rlaaprise.rajasthan.gov.in"
HOUSE_PROCEEDINGS_URL = f"{BASE_URL}/HouseProceedingView.aspx"

class RajasthanLegislatureMirror:
    def __init__(self):
        self.session = requests.Session()
        # Disable SSL verification
        self.session.verify = False
        # Add User-Agent header and other headers to match HAR file exactly
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
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
        
        # Store ASP.NET session state
        self.viewstate = None
        self.viewstate_generator = None
        self.event_validation = None
        self.viewstate_encrypted = None
        
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
        """Search Internet Archive for existing Rajasthan Legislature documents"""
        try:
            logger.info("Searching Internet Archive for existing documents...")
            
            # Search for documents with our creator
            search_query = 'creator:"Rajasthan Legislative Assembly"'
            search_results = ia.search_items(search_query)
            
            existing_count = 0
            for item in search_results:
                identifier = item.get('identifier', '')
                
                # Parse identifier to extract document info
                # Expected format: rajasthanlegislature.{house}.{session}.{date}.{filename}
                if identifier.startswith('rajasthanlegislature.'):
                    # Mark as processed to avoid re-uploading
                    self.processed_docs.add(identifier)
                    existing_count += 1
            
            logger.info(f"Found {existing_count} existing documents on Internet Archive")
            
        except Exception as e:
            logger.warning(f"Error searching Internet Archive: {e}")
            logger.info("Continuing without Internet Archive search results")
    
    def fetch_initial_page(self):
        """Fetch the initial house proceedings page and extract ASP.NET state"""
        try:
            logger.info("Fetching initial house proceedings page...")
            
            response = self.session.get(HOUSE_PROCEEDINGS_URL, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract ASP.NET viewstate and other form parameters
            viewstate_input = soup.find('input', {'id': '__VIEWSTATE'})
            self.viewstate = viewstate_input['value'] if viewstate_input else None
            
            viewstate_gen_input = soup.find('input', {'id': '__VIEWSTATEGENERATOR'})
            self.viewstate_generator = viewstate_gen_input['value'] if viewstate_gen_input else None
            
            event_val_input = soup.find('input', {'id': '__EVENTVALIDATION'})
            self.event_validation = event_val_input['value'] if event_val_input else None
            
            viewstate_enc_input = soup.find('input', {'id': '__VIEWSTATEENCRYPTED'})
            self.viewstate_encrypted = viewstate_enc_input['value'] if viewstate_enc_input else ''
            
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return soup
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching initial page: {e}")
            return None
    
    def update_aspnet_state(self, soup):
        """Update ASP.NET viewstate and event validation from response"""
        viewstate_input = soup.find('input', {'id': '__VIEWSTATE'})
        if viewstate_input:
            self.viewstate = viewstate_input['value']
            
        viewstate_gen_input = soup.find('input', {'id': '__VIEWSTATEGENERATOR'})
        if viewstate_gen_input:
            self.viewstate_generator = viewstate_gen_input['value']
            
        event_val_input = soup.find('input', {'id': '__EVENTVALIDATION'})
        if event_val_input:
            self.event_validation = event_val_input['value']
            
        viewstate_enc_input = soup.find('input', {'id': '__VIEWSTATEENCRYPTED'})
        if viewstate_enc_input:
            self.viewstate_encrypted = viewstate_enc_input['value']
    
    def parse_assembly_session_info(self, session_text):
        """
        Parse Assembly Number and Session Number from session text.
        
        Expected formats:
        - '1th Assembly, XI Session [ 03 Dec 1956 - 12 Dec 1956 ] - 9 Sittings'
        - '2nd Assembly, I Session [ 15 Feb 1957 - 28 Mar 1957 ] - 15 Sittings'
        - '15th Assembly, VII Session [ 12 Feb 2018 - 23 Mar 2018 ] - 20 Sittings'
        
        Returns:
            dict: {'assembly_number': int, 'session_number': str, 'assembly_ordinal': str, 'session_roman': str}
        """
        if not session_text:
            return {'assembly_number': None, 'session_number': None, 'assembly_ordinal': None, 'session_roman': None}
        
        try:
            # Clean up HTML tags (like <sup>th</sup>) first
            clean_text = re.sub(r'<[^>]+>', '', session_text)
            
            # Pattern to match: "{number}{ordinal} Assembly, {roman_numeral} Session"
            pattern = r'(\d+)(st|nd|rd|th)\s+Assembly,\s+([IVX]+)\s+Session'
            match = re.search(pattern, clean_text, re.IGNORECASE)
            
            if match:
                assembly_num = int(match.group(1))
                assembly_ordinal = f"{match.group(1)}{match.group(2)}"
                session_roman = match.group(3)
                
                # Convert Roman numeral to integer
                roman_to_int = {
                    'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5, 'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10,
                    'XI': 11, 'XII': 12, 'XIII': 13, 'XIV': 14, 'XV': 15, 'XVI': 16, 'XVII': 17, 'XVIII': 18, 'XIX': 19, 'XX': 20,
                    'XXI': 21, 'XXII': 22, 'XXIII': 23, 'XXIV': 24, 'XXV': 25
                }
                
                session_num = roman_to_int.get(session_roman.upper())
                
                return {
                    'assembly_number': assembly_num,
                    'session_number': session_num,
                    'assembly_ordinal': assembly_ordinal,
                    'session_roman': session_roman
                }
            else:
                logger.warning(f"Could not parse assembly/session info from: {session_text}")
                return {'assembly_number': None, 'session_number': None, 'assembly_ordinal': None, 'session_roman': None}
                
        except Exception as e:
            logger.error(f"Error parsing assembly/session info from '{session_text}': {e}")
            return {'assembly_number': None, 'session_number': None, 'assembly_ordinal': None, 'session_roman': None}
    
    def get_sessions_for_house(self, house_id):
        """Get all sessions for a specific house"""
        try:
            logger.info(f"Getting sessions for house {house_id}...")
            
            # Prepare form data for house selection
            form_data = {
                '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$DDLHouse',
                '__EVENTARGUMENT': '',
                '__LASTFOCUS': '',
                '__VIEWSTATE': self.viewstate,
                '__VIEWSTATEGENERATOR': self.viewstate_generator,
                '__VIEWSTATEENCRYPTED': self.viewstate_encrypted or '',
                '__EVENTVALIDATION': self.event_validation,
                'ctl00$ContentPlaceHolder1$DDLHouse': str(house_id)
            }
            
            # Set proper headers for POST request
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Referer': HOUSE_PROCEEDINGS_URL
            }
            
            response = self.session.post(HOUSE_PROCEEDINGS_URL, data=form_data, headers=headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Update ASP.NET state
            self.update_aspnet_state(soup)
            
            # Extract session information from HTML spans and create session groups
            sessions = []
            
            # Find all session info spans (pattern: ctl00_ContentPlaceHolder1_Outer_ctlXX_LblInfo)
            session_spans = soup.find_all('span', {'id': lambda x: x and 'LblInfo' in x and 'Outer' in x})
            
            for i, session_span in enumerate(session_spans):
                session_text = session_span.get_text(strip=True)
                
                # Find corresponding house span
                house_span_id = session_span.get('id').replace('LblInfo', 'LblHouse')
                house_span = soup.find('span', {'id': house_span_id})
                house_text = house_span.get_text(strip=True) if house_span else f"House {house_id}"
                
                # Parse assembly and session information
                session_info = self.parse_assembly_session_info(f"{house_text} {session_text}")
                
                # Find the corresponding DataList table for this session
                datalist_id_pattern = session_span.get('id').replace('LblInfo', 'DataList1')
                datalist = soup.find('table', {'id': datalist_id_pattern})
                
                documents = []
                if datalist:
                    # Find all LinkButton2 elements (PDF download buttons)
                    link_buttons = datalist.find_all('a', {'id': lambda x: x and 'LinkButton2' in x})
                    
                    # If no LinkButton2, try to find any links with onclick
                    if not link_buttons:
                        all_links = datalist.find_all('a', onclick=True)
                        link_buttons = all_links
                    
                    for link_button in link_buttons:
                        # Extract the event target for this link
                        onclick = link_button.get('onclick', '')
                        href = link_button.get('href', '')
                        
                        if '__doPostBack' in onclick or '__doPostBack' in href:
                            # Extract the control ID from the onclick event or href
                            event_target_match = re.search(r"'([^']+)'", onclick or href)
                            if event_target_match:
                                event_target = event_target_match.group(1)
                                
                                # The date is in the link text itself
                                date_text = link_button.get_text(strip=True)
                                
                                if date_text and date_text != 'unknown_date':
                                    # Create document info with session information
                                    document = {
                                        'house_id': house_id,
                                        'session_id': f'session_{i+1}',
                                        'event_target': event_target,
                                        'date_text': date_text,
                                        'identifier': f"rajasthanlegislature.assembly{house_id}.session{session_info['session_number']}.{date_text.replace('/', '-')}",
                                        'assembly_number': session_info['assembly_number'],
                                        'session_number': session_info['session_number'],
                                        'assembly_ordinal': session_info['assembly_ordinal'],
                                        'session_roman': session_info['session_roman'],
                                        'session_name': session_text,
                                        'house_text': house_text
                                    }
                                    documents.append(document)
                
                # Create session object with embedded documents
                session_data = {
                    'id': f'session_{i+1}',
                    'name': session_text,
                    'assembly_number': session_info['assembly_number'],
                    'session_number': session_info['session_number'],
                    'assembly_ordinal': session_info['assembly_ordinal'],
                    'session_roman': session_info['session_roman'],
                    'house_text': house_text,
                    'documents': documents
                }
                sessions.append(session_data)
                
                logger.info(f"Found session: {session_text} with {len(documents)} documents")
            
            if sessions:
                logger.info(f"Found {len(sessions)} sessions for house {house_id}")
                return sessions
            
            # If no sessions found, return empty list
            logger.warning(f"No sessions found for house {house_id}")
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return []
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting sessions for house {house_id}: {e}")
            return []
    
    def get_documents_for_session(self, house_id, session_id, session_info=None):
        """Get all documents for a specific house and session"""
        try:
            logger.info(f"Getting documents for house {house_id}, session {session_id}...")
            
            # Prepare form data for session selection
            form_data = {
                '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$DDLSession',
                '__EVENTARGUMENT': '',
                '__LASTFOCUS': '',
                '__VIEWSTATE': self.viewstate,
                '__VIEWSTATEGENERATOR': self.viewstate_generator,
                '__EVENTVALIDATION': self.event_validation,
                'ctl00$ContentPlaceHolder1$DDLHouse': str(house_id),
                'ctl00$ContentPlaceHolder1$DDLSession': str(session_id)
            }
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Referer': HOUSE_PROCEEDINGS_URL
            }
            
            response = self.session.post(HOUSE_PROCEEDINGS_URL, data=form_data, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Update ASP.NET state
            self.update_aspnet_state(soup)
            
            # Find all PDF download links in the DataList
            documents = []
            datalist = soup.find('table', {'id': lambda x: x and 'DataList1' in x})
            
            if datalist:
                # Find all LinkButton2 elements (PDF download buttons)
                link_buttons = datalist.find_all('a', {'id': lambda x: x and 'LinkButton2' in x})
                
                for link_button in link_buttons:
                    # Extract the event target for this link
                    onclick = link_button.get('onclick', '')
                    if '__doPostBack' in onclick:
                        # Extract the control ID from the onclick event
                        event_target_match = re.search(r"'([^']+)'", onclick)
                        if event_target_match:
                            event_target = event_target_match.group(1)
                            
                            # Find the associated date text (usually in a nearby cell)
                            row = link_button.find_parent('tr')
                            if row:
                                # Look for date information in the row
                                date_cell = row.find('td')
                                date_text = date_cell.get_text(strip=True) if date_cell else 'unknown_date'
                                
                                # Create document info
                                document = {
                                    'house_id': house_id,
                                    'session_id': session_id,
                                    'event_target': event_target,
                                    'date_text': date_text,
                                    'identifier': f"rajasthanlegislature.assembly{house_id}.session{session_info['session_number']}.{date_text.replace('/', '-')}"
                                }
                                
                                # Add assembly and session information if available
                                if session_info:
                                    document.update({
                                        'assembly_number': session_info.get('assembly_number'),
                                        'session_number': session_info.get('session_number'),
                                        'assembly_ordinal': session_info.get('assembly_ordinal'),
                                        'session_roman': session_info.get('session_roman'),
                                        'session_name': session_info.get('name')
                                    })
                                
                                documents.append(document)
            
            logger.info(f"Found {len(documents)} documents for session {session_id}")
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return documents
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting documents for session {session_id}: {e}")
            return []
    
    def download_document(self, document):
        """Download a legislative document PDF by triggering the ASP.NET postback"""
        try:
            logger.info(f"Downloading document for {document['date_text']}...")
            
            # Prepare form data to trigger PDF download
            form_data = {
                '__EVENTTARGET': document['event_target'],
                '__EVENTARGUMENT': '',
                '__LASTFOCUS': '',
                '__VIEWSTATE': self.viewstate,
                '__VIEWSTATEGENERATOR': self.viewstate_generator,
                '__VIEWSTATEENCRYPTED': self.viewstate_encrypted or '',
                '__EVENTVALIDATION': self.event_validation,
                'ctl00$ContentPlaceHolder1$DDLHouse': str(document['house_id'])
            }
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Referer': HOUSE_PROCEEDINGS_URL
            }
            
            response = self.session.post(HOUSE_PROCEEDINGS_URL, data=form_data, headers=headers, timeout=60, stream=True)
            response.raise_for_status()
            
            # Check if response is actually a PDF
            content_type = response.headers.get('content-type', '')
            if 'application/pdf' in content_type:
                # Generate filename based on document info
                filename = f"assembly{document['house_id']}_{document['session_id']}_{document['date_text'].replace('/', '-')}.pdf"
                filepath = self.document_dir / filename
                
                # Check if already exists
                if filepath.exists():
                    logger.info(f"Document already exists: {filename}")
                    return filepath
                
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                logger.info(f"Downloaded document: {filename}")
                document['filename'] = filename  # Update document with actual filename
                time.sleep(SLEEP_BETWEEN_REQUESTS)
                return filepath
            else:
                # Response is HTML - look for JavaScript window.open with PDF URL
                soup = BeautifulSoup(response.text, 'html.parser')
                self.update_aspnet_state(soup)
                
                # Look for window.open JavaScript with PDF URL
                pdf_url_match = re.search(r"window\.open\(['\"]\./(.*?\.pdf)['\"]", response.text)
                
                if pdf_url_match:
                    pdf_path = pdf_url_match.group(1)
                    pdf_url = urljoin(BASE_URL + '/', pdf_path)
                    logger.info(f"Found PDF URL in JavaScript: {pdf_url}")
                    
                    # Download the PDF directly
                    pdf_response = self.session.get(pdf_url, timeout=60, stream=True)
                    pdf_response.raise_for_status()
                    
                    # Check if this response is actually a PDF
                    pdf_content_type = pdf_response.headers.get('content-type', '')
                    if 'application/pdf' in pdf_content_type:
                        # Generate filename based on document info
                        filename = f"{document['date_text'].replace('/', '-')}.pdf"
                        filepath = self.document_dir / filename
                        
                        # Check if already exists
                        if filepath.exists():
                            logger.info(f"Document already exists: {filename}")
                            return filepath
                        
                        with open(filepath, 'wb') as f:
                            for chunk in pdf_response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        
                        logger.info(f"Downloaded PDF from {pdf_url}: {filename}")
                        document['filename'] = filename
                        document['source_url'] = pdf_url
                        time.sleep(SLEEP_BETWEEN_REQUESTS)
                        return filepath
                    else:
                        logger.warning(f"PDF URL returned non-PDF content: {pdf_content_type}")
                        return None
                else:
                    return None
            
        except Exception as e:
            logger.error(f"Error downloading document {document['date_text']}: {e}")
            return None
    
    def save_document_metadata(self, document):
        """Save document metadata to JSON file"""
        if 'filename' not in document:
            logger.error("Document missing filename, cannot save metadata")
            return None
            
        filename = f"{document['filename'].replace('.pdf', '')}.json"
        filepath = self.metadata_dir / filename
        
        try:
            # Prepare metadata with processing timestamp
            metadata = {
                **document,
                'processing_date': datetime.now().isoformat(),
                'source_url': document['source_url']
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
                'creator': 'Rajasthan Legislative Assembly',
                'source': document['source_url'],
                'mediatype': 'texts',
                'language': ['Hindi', 'English'],
                'licenseurl': 'http://creativecommons.org/licenses/publicdomain/',
                'subject': ['Rajasthan Legislative Assembly'],
                'collection': 'parliamentofindia'
            }
            
            # Add extracted metadata
            if metadata_path and metadata_path.exists():
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    doc_metadata = json.load(f)

                ia_metadata['title'] = f"Assembly {document['assembly_number']}, Session {document['session_number']}, {document['date_text']}"
                ia_metadata['description'] = f"Rajasthan Legislative Assembly proceedings for Assembly {document['assembly_number']}, Session {document['session_number']} on {document['date_text']}"
                
                # Extract date from date_text if possible
                date_match = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{4})', document['date_text'])
                if date_match:
                    ia_metadata['date'] = date_match.group(1)
                
                # Add specific fields from document metadata
                for key, value in doc_metadata.items():
                    if key not in ['source_url', 'processing_date', 'filename', 'event_target', 'assembly_ordinal', 'session_roman', 'house_id', 'session_id', 'date_text', 'identifier', 'session_name', 'house_text']:
                        ia_metadata[f"rajasthan_legislature_{key}"] = value

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
        
        # Check if already processed
        already_processed = identifier in self.processed_docs
        
        if already_processed:
            logger.info(f"Document already processed: {identifier}")
            return {
                'exists': True,
                'already_processed': True,
                'pdf_exists': False,
                'metadata_exists': False,
                'pdf_path': None,
                'metadata_path': None
            }
        
        # If we don't have a filename yet, we can't check file existence
        if 'filename' not in document:
            return {
                'exists': False,
                'already_processed': False,
                'pdf_exists': False,
                'metadata_exists': False,
                'pdf_path': None,
                'metadata_path': None
            }
        
        # Check if PDF file exists
        pdf_filename = document['filename']
        pdf_filepath = self.document_dir / pdf_filename
        
        # Check if metadata file exists
        metadata_filename = f"{document['filename'].replace('.pdf', '')}.json"
        metadata_filepath = self.metadata_dir / metadata_filename
        
        # Log the status
        if pdf_filepath.exists() and metadata_filepath.exists():
            logger.info(f"Document files already exist: {identifier} (PDF: {pdf_filename}, Metadata: {metadata_filename})")
        elif pdf_filepath.exists():
            logger.info(f"PDF file already exists: {identifier} (PDF: {pdf_filename})")
        elif metadata_filepath.exists():
            logger.info(f"Metadata file already exists: {identifier} (Metadata: {metadata_filename})")
        
        return {
            'exists': pdf_filepath.exists() and metadata_filepath.exists(),
            'already_processed': already_processed,
            'pdf_exists': pdf_filepath.exists(),
            'metadata_exists': metadata_filepath.exists(),
            'pdf_path': pdf_filepath,
            'metadata_path': metadata_filepath
        }
    
    def check_archive_org_exists(self, document):
        """Check if document is already archived on archive.org"""
        try:
            # Use the document's identifier directly
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
            
            # Step 2: Check if document files already exist locally (only if we have a filename)
            doc_status = self.check_document_exists(document)
            
            if doc_status['already_processed']:
                logger.info(f"Document already processed: {identifier}")
                return True
            
            logger.info(f"Processing new document: {identifier}")
            
            # Step 3: Download PDF (this will also set the filename)
            pdf_path = self.download_document(document)
            if not pdf_path:
                logger.warning(f"Failed to download PDF for {identifier}")
                return False
            
            # Step 4: Save metadata (now that we have filename)
            metadata_path = self.save_document_metadata(document)
            if not metadata_path:
                logger.warning(f"Failed to save metadata for {identifier}")
                return False
            
            # Step 5: Upload to Internet Archive
            upload_success = self.upload_to_internet_archive(document, metadata_path, pdf_path)
            
            # Mark as processed if successful
            if upload_success:
                self.processed_docs.add(identifier)
                self.save_processed_docs()
                logger.info(f"Successfully processed document: {identifier}")

                # Delete the PDF file to save space
                if pdf_path.exists():
                    pdf_path.unlink()
                    logger.info(f"Deleted PDF file: {pdf_path}")

                # Delete the metadata file to save space
                if metadata_path.exists():
                    metadata_path.unlink()
                    logger.info(f"Deleted metadata file: {metadata_path}")
            
            return upload_success
            
        except Exception as e:
            logger.error(f"Error processing document: {e}")
            return False
    
    def process_all_documents(self):
        """Process all documents from the Rajasthan Legislative Assembly"""
        logger.info("Starting to process all Rajasthan Legislative Assembly documents")
        
        # Step 1: Fetch initial page and extract ASP.NET state
        soup = self.fetch_initial_page()
        if not soup:
            logger.error("Failed to fetch initial page")
            return False
        
        # Step 2: Process all houses (House 1 first, then up to House 16)
        houses_to_process = list(range(1, 17))  # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        
        all_documents = []
        
        for house_id in houses_to_process:
            logger.info(f"Processing house {house_id}...")
            
            # Get all sessions for this house
            sessions = self.get_sessions_for_house(house_id)
            if not sessions:
                logger.warning(f"No sessions found for house {house_id}")
                continue
            
            for session in sessions:
               
                # Check if documents are already included with the session
                if 'documents' in session:
                    documents = session['documents']
                    all_documents.extend(documents)
                else:
                    # Get all documents for this session
                    documents = self.get_documents_for_session(house_id, session['id'], session)
                    if documents:
                        all_documents.extend(documents)
                        logger.info(f"Found {len(documents)} documents in session {session['id']}")
                    else:
                        logger.warning(f"No documents found in session {session['id']}")
        
        if not all_documents:
            logger.error("No documents found to process")
            return False
        
        logger.info(f"Found {len(all_documents)} total documents to process")
        
        # Step 3: Process each document
        success_count = 0
        total_count = len(all_documents)
        
        for i, document in enumerate(all_documents, 1):
            logger.info(f"Processing document {i}/{total_count}: {document['identifier']}")
            
            if self.process_document(document):
                success_count += 1
            
            # Save progress periodically
            if i % 5 == 0:
                self.save_processed_docs()
                logger.info(f"Progress: {i}/{total_count} documents processed, {success_count} successful")
        
        logger.info(f"Processing complete: {success_count}/{total_count} documents successfully processed")
        return success_count > 0
    
    def run(self):
        """Main execution function"""
        logger.info("Starting Rajasthan Legislative Assembly Mirror Tool")
        
        try:
            # Process all documents from the house proceedings system
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
    mirror = RajasthanLegislatureMirror()
    mirror.run()


if __name__ == "__main__":
    main()
