This repository contains multiple scripts that have been made to download all hosted files on the TamilNadu Legislative Assembly files repository. It is then mirrored using the Internet Archive.

## Scripts

- `debates_table_Scraper.py` scrapes and saves a list of files to a csv, `tn_digital_data.csv`
- `init.py` handles `downloader_.py` and `uploader_parallel.py` parallely.
- `downloader_.py` downloads based on `tn_digital_data.csv` and saves to a folder and adds metadata to `queue/`
- `uploader_parallel` takes from `queue/` files and uploads in parallel to the Internet Archive