# Kerala Legislative Assembly Mirror Tool

This tool archives documents from the Kerala Legislative Assembly to Internet Archive.

## Features

- Fetches assembly options from the advanced search page
- Iterates through each assembly option and all pages
- Extracts document metadata and IDs from HTML responses
- Downloads member lists for each document
- Downloads document PDFs
- Uploads documents with metadata to Internet Archive
- Tracks progress to avoid re-processing
- Automatically cleans up local PDF files after successful upload
- Searches Internet Archive for existing documents to avoid duplicates

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure Internet Archive credentials:
```bash
ia configure
```

## Usage

Run the mirror tool:
```bash
python3 mirror.py
```

## API Endpoints

- **Advanced Search Page**: `http://klaproceedings.niyamasabha.org/index.php?pg=advanced_search_combo`
- **Search Results API**: `http://klaproceedings.niyamasabha.org/adv_search_result.php`
- **Member List API**: `http://klaproceedings.niyamasabha.org/ListSearchMembers.php?memberList={doc_id}`
- **Document PDF API**: `http://klaproceedings.niyamasabha.org/docs_to_pdf.php?memberList={doc_id}`

## Directory Structure

```
raw/
├── list/           # HTML search results for each assembly/page
└── metadata/       # Document metadata (JSON files)
documents/          # Downloaded PDFs (cleaned up after successful upload)
```

## Progress Tracking

The tool tracks progress in three files:
- `processed_assemblies.json`: Assembly/page combinations that have been processed
- `processed_documents.json`: Documents that have been processed (may include failed uploads)
- `uploaded_documents.json`: Documents that have been successfully uploaded to Internet Archive

## Document Metadata Extraction

For each document, the tool extracts:
- **ID**: Document identifier
- **Date**: Document date (DD-MM-YYYY format)
- **Assembly**: Assembly information (e.g., "KLA - 15 (2021-2026)")
- **Session**: Session number
- **Event**: Event type (English and Malayalam)
- **Subject**: Subject/topic (English and Malayalam)
- **Members**: List of members involved (if available)
- **PDF availability**: Whether PDF is available for download

## Internet Archive Metadata

Documents are uploaded with the following metadata:
- Creator: "Kerala Legislative Assembly Secretariat"
- Language: English, Malayalam
- Subject: Kerala Legislative Assembly, Legislative Proceedings
- Title format: "Kerala Legislative Assembly - {subject}"
- Description: Includes date, assembly, session, event, and subject information

## Search and Pagination

The tool:
1. Fetches all assembly options from the advanced search page
2. For each assembly, processes pages sequentially starting from page 1
3. Continues until no more pages are available
4. Uses pagination detection to determine when to stop

## Storage Optimization

The tool optimizes local storage by:
- Downloading PDFs only when needed for upload
- Automatically deleting local PDF files after successful upload to Internet Archive
- Retaining only metadata JSON files locally for reference
- Checking Internet Archive for existing documents before processing

## Configuration

- **Sleep Between Requests**: 1.0 seconds (configurable in `SLEEP_BETWEEN_REQUESTS`)
- **Maximum Pages**: 1000 per assembly (safety limit)

## Debugging Features

The tool includes comprehensive debugging capabilities:
- **Curl Equivalents**: Every HTTP request is logged with its curl equivalent, including all headers, data, and cookies. You can copy these commands directly to your terminal for manual testing.
- **Request Timing**: Configurable timeout values for different request types
- **Progress Tracking**: Three-tier progress tracking system
- **Internet Archive Search**: Logs search queries when checking for existing documents

### Example Curl Output
```
Curl equivalent: curl -X POST -H 'Content-Type: application/x-www-form-urlencoded' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:140.0) Gecko/20100101 Firefox/140.0' -H 'Accept: */*' -H 'Accept-Language: en-US,en;q=0.5' -H 'Accept-Encoding: gzip, deflate' -H 'Connection: keep-alive' -H 'Referer: http://klaproceedings.niyamasabha.org/index.php?pg=advanced_search_combo' -H 'Origin: http://klaproceedings.niyamasabha.org' -H 'X-Requested-With: XMLHttpRequest' --data-raw 'FlagPost=1^&assembly=15^&session=^&date_search=^&date_search1=^&class_search=^&member=^&subject=^&lang=eng^&curpage=1^&form_token=79948d8abea0218b0abad9e665bd8edd' 'http://klaproceedings.niyamasabha.org/adv_search_result.php'
```

## Logging

All activities are logged to `mirror.log` with detailed information about:
- Assembly processing progress
- Document extraction and processing
- Internet Archive upload status
- Local file cleanup after successful uploads
- Error handling and recovery
- Progress tracking and duplicate detection
- Curl equivalents of all HTTP requests for debugging

## Error Handling

The tool includes robust error handling:
- Continues processing if individual documents fail
- Saves progress periodically (assemblies, processed documents, and uploaded documents)
- Graceful handling of network timeouts
- Skips already uploaded documents to avoid duplicates
- Searches Internet Archive for existing documents on startup
- Distinguishes between processed and successfully uploaded documents
- Preserves local files if upload fails for retry attempts 

## AI Declaration

Components of this repository, including code and documentation, were written with assistance from Claude AI.
