# Karnataka Legislative Assembly Mirror Tool

This tool archives documents from the Karnataka Legislative Assembly to Internet Archive.

## Features

- Fetches document lists for each day from 1952-06-18 to today
- Saves daily document lists as JSON files in `raw/list/`
- Extracts debate results and metadata
- Downloads debate PDF documents to `raw/document/`
- Uploads to Internet Archive with proper metadata

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure Internet Archive credentials (optional, for uploading):
```bash
ia configure
```

## Usage

### Run the full mirror tool:
```bash
python3 mirror.py
```

### Test with a single date:
```bash
python3 test_single_date.py
```

## API Endpoints

- **Document List API**: `http://103.138.196.55:9200/api/sd/sh`
- **Document Download API**: `http://103.138.196.55:9200/api/fs/section/debates/kla/{bookId}/{startPage}/{endPage}`

## Directory Structure

```
raw/
├── list/           # Daily document lists (JSON files)
├── document/       # Downloaded debate PDFs
└── metadata/       # Extracted metadata (JSON files)
```

## Progress Tracking

The tool tracks progress in two files:
- `processed_dates.json`: Dates that have been processed
- `processed_documents.json`: Documents that have been uploaded to Internet Archive

## Configuration

- **Start Date**: Default is 1952-06-18 (can be modified in `generate_date_range` method)
- **Sleep Between Requests**: 1.0 seconds (configurable in `SLEEP_BETWEEN_REQUESTS`)
- **Batch Size**: 10 dates per batch (configurable in `BATCH_SIZE`)

## Internet Archive Metadata

Documents are uploaded with the following metadata:
- Creator: "Karnataka Legislative Assembly"
- Language: English, Kannada
- Subject: Karnataka Legislative Assembly
- Title format: "Karnataka Legislative Assembly Debates - Book {bookId}, Pages {startPage}-{endPage}"

## Logging

All activities are logged to:
- `mirror.log` (main script)
- `test_mirror.log` (test script)

## Error Handling

The tool includes robust error handling:
- Continues processing if individual documents fail
- Saves progress periodically
- Cleans up files to manage disk space
- Graceful handling of API timeouts and network issues 

## AI Declaration

Components of this repository, including code and documentation, were written with assistance from Claude AI.
