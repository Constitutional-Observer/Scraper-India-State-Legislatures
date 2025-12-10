# Telangana Legislature Mirror Tool

This tool archives documents from the Telangana Legislature to Internet Archive, focusing specifically on Telangana-related documents and excluding those from Hyderabad or Andhra Pradesh.

## Features

- Parses the hierarchical archives tree structure (Assembly/Council > Terms > Sessions > Sittings)
- Extracts document metadata and download URLs from the nested treeview
- Filters documents to include only Telangana-specific content, excluding Hyderabad and Andhra Pradesh documents
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
1. Fetch the archives tree from Telangana Legislature website
2. Parse the hierarchical structure to extract all document URLs
3. Filter documents to include only Telangana-specific content
4. Download each PDF document
5. Create metadata files
6. Upload everything to Internet Archive

## Data Sources

- **Archives Tree Page**: `https://legislature.telangana.gov.in/debates`
- **Document Download Base**: `https://sessions-legislature.telangana.gov.in/PreviewPage.do`

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
- **Archives URL**: `https://legislature.telangana.gov.in/debates`
- **Document Filtering**: Automatically excludes documents from Andhra Pradesh legislature and pre-bifurcation Hyderabad documents

## Internet Archive Metadata

Documents are uploaded with the following metadata:
- Creator: "Telangana State Legislature"
- Language: English, Telugu
- Subject: Telangana State Legislature, Assembly/Council
- Title format: "Telangana {House} - {Day}"
- Description: "Telangana State Legislature {House} proceedings - {Term}, {Session}, {Sitting}, {Day}"

## Document Structure

The Telangana Legislature archives are organized hierarchically:

1. **Assembly/Council**: Top-level categorization
2. **Terms**: Different legislative terms (e.g., "First Telangana Legislative Assembly (2014-2018)")
3. **Sessions**: Individual sessions within each term (e.g., "Session 1", "Session 2")
4. **Sittings**: Sitting periods within sessions (e.g., "sitting 1(09-06-2014 to 14-06-2014)")
5. **Days**: Individual day proceedings as PDF documents (e.g., "Day (09-06-2014)")

## Document Filtering

The tool automatically filters documents to include only Telangana-specific content:

- **Included**: Documents from Telangana State Legislature (post-2014)
- **Excluded**: Documents from Andhra Pradesh Legislature (aplegislature.org)
- **Excluded**: Pre-bifurcation documents (before 2014)
- **Excluded**: Documents mentioning Andhra Pradesh in titles or metadata

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
