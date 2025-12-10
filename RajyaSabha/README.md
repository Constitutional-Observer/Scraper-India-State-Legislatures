# ePARLIB Mirror Tool

This tool archives documents from the Parliament Digital Library (eparlib.nic.in) by:
1. Downloading document detail pages
2. Extracting metadata from HTML pages
3. Downloading PDF documents
4. Uploading to Internet Archive with metadata

## Setup

1. Install Python 3.8 or higher
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure Internet Archive credentials:
   ```bash
   ia configure
   ```

## Usage

Run the script:
```bash
python mirror.py
```

The script will:
- Process documents with IDs from 1 to 30,000,000
- Save HTML pages in `raw/htmls/`
- Save PDFs in `output/documents/`
- Save metadata in `output/metadata/`
- Upload documents to Internet Archive with prefix `eparlib-`
- Stop if output directory exceeds 10GB
- Track progress in `processed_documents.json`

You can interrupt the script at any time with Ctrl+C. It will save progress and can be resumed from where it left off.

## Output Structure

```
.
├── raw/
│   └── htmls/           # Raw HTML pages
├── output/
│   ├── documents/       # Downloaded PDFs
│   └── metadata/        # Extracted metadata (JSON)
├── mirror.log           # Log file
└── processed_documents.json  # Progress tracking
```

## Internet Archive Collection

Documents are uploaded to Internet Archive with:
- Identifier format: `eparlib-{doc_id}`
- Collection: `india_parliament_digital_library`
- Creator: "Parliament Digital Library"
- Source: https://eparlib.nic.in/ 

## AI Declaration

Components of this repository, including code and documentation, were written with assistance from Claude AI.
