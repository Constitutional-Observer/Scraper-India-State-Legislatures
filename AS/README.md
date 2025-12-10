# Andhra Pradesh Legislature Mirror Tool

This tool archives documents from the Andhra Pradesh Legislature to Internet Archive.

## Features

- Parses the hierarchical archives tree structure (Assembly/Council > Terms > Sessions > Sittings)
- Extracts document metadata and download URLs from the nested treeview
- Downloads legislative debate PDF documents to `raw/document/`
- Saves structured metadata as JSON files in `raw/metadata/`
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

This will:
1. Fetch the archives tree from AP Legislature website
2. Parse the hierarchical structure to extract all document URLs
3. Download each PDF document
4. Create metadata files
5. Upload everything to Internet Archive

## Data Sources

- **Archives Tree Page**: `https://www.aplegislature.org/web/aplegislature/archives-tree`
- **Document Download Base**: `https://sessions.aplegislature.org/preview.do`

## Directory Structure

```
raw/
├── tree/           # Downloaded archives tree HTML
├── document/       # Downloaded legislative PDFs
└── metadata/       # Extracted metadata (JSON files)
```

## Progress Tracking

The tool tracks progress in:
- `processed_documents.json`: Documents that have been uploaded to Internet Archive

## Configuration

- **Sleep Between Requests**: 1.0 seconds (configurable in `SLEEP_BETWEEN_REQUESTS`)
- **Archives URL**: `https://www.aplegislature.org/web/aplegislature/archives-tree`

## Internet Archive Metadata

Documents are uploaded with the following metadata:
- Creator: "Andhra Pradesh Legislature Secretariat"
- Language: English, Telugu
- Subject: Andhra Pradesh Legislature, Assembly/Council
- Title format: "AP Legislature {House} - {Day}"
- Description: "Andhra Pradesh Legislature {House} proceedings - {Term}, {Session}, {Sitting}, {Day}"

## Document Structure

The AP Legislature archives are organized hierarchically:

1. **Assembly/Council**: Top-level categorization
2. **Terms**: Different legislative terms (e.g., "Hyderabad Legislative Assembly (1st to 8th & 10th Sessions)")
3. **Sessions**: Individual sessions within each term (e.g., "Session 1", "Session 2")
4. **Sittings**: Sitting periods within sessions (e.g., "sitting 1(21/03/1952 to 07/04/1952)")
5. **Days**: Individual day proceedings as PDF documents (e.g., "Day 1(21/03/1952)")

## Logging

All activities are logged to:
- `mirror.log` (main script)

## Error Handling

The tool includes robust error handling:
- Continues processing if individual documents fail
- Saves progress periodically
- Cleans up files to manage disk space
- Graceful handling of network timeouts and parsing errors 

## AI Declaration

Components of this repository, including code and documentation, were written with assistance from Claude AI.
