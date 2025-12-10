import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import os

def find_target_table(soup):
    """Find the table that contains Assembly No, Session No headers"""
    target_headers = ['Assembly No', 'Session No']
    
    # Find all tables
    tables = soup.find_all('table')
    
    for table in tables:
        # Check if this table contains our target headers
        header_row = table.find('tr')
        if header_row:
            header_cells = header_row.find_all(['th', 'td'])
            header_texts = [cell.get_text(strip=True) for cell in header_cells]
            
            # Check if any of our target headers are present
            if any(header in header_texts for header in target_headers):
                return table, header_texts
    
    return None, []

def scrape_page(start=0):
    """Scrape a single page and return the data"""
    url = f"https://tnlasdigital.tn.gov.in/jspui/simple-search?query=&location=123456789%2F100&sort_by=dc.date_dt&order=asc&rpp=100&etal=0&start={start}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the correct table using header identification
        table, header_texts = find_target_table(soup)
        
        if not table:
            print(f"Could not find table with Assembly No/Session No headers at start={start}")
            return []
        
        data = []
        
        # Get all rows except the header row
        rows = table.find_all('tr')[1:]  # Skip header row
        link_no = 1
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:  # Must have at least 2 columns
                
                # Extract text from all cells
                cell_texts = [cell.get_text(strip=True) for cell in cells]
                
                # Extract all links in the row with their alt text or text content
                links_with_names = []
                for idx,link in enumerate(row.find_all('a', href=True)):
                    full_url = requests.compat.urljoin(url, link['href'])
                    print(full_url)
                    # Get link name from alt attribute, title, or text content
                    link_name = (link.get('alt') or
                               link.get('title') or
                               link.get_text(strip=True) or 
                               'link'+str(idx))
                    
                    # Clean the link name for CSV column compatibility
                    clean_link_name = link_name.replace(' ', '_').replace('.', '').replace('(', '').replace(')', '').lower()
                    links_with_names.append((clean_link_name, full_url))
                    #print(links_with_names)
                # Create record using actual header names
                record = {}
                
                # Map cell data to header names (excluding action field)
                for i, header in enumerate(header_texts):
                    if i < len(cell_texts) and header.lower() != 'action':
                        # Clean header name for CSV compatibility
                        clean_header = header.replace(' ', '_').replace('.', '').replace('(', '').replace(')', '').lower()
                        record[clean_header] = cell_texts[i]
                
                # Add links using their alt text as field names
                for link_name, link_url in links_with_names:
                    record[link_name] = link_url
                
                record['page_start'] = start
                
                # Only add if we have some data
                if any(record.get(key, '') for key in record if key not in ['page_start'] and not key.startswith('link_')):
                    data.append(record)
        return data
        
    except Exception as e:
        print(f"Error scraping page {start}: {e}")
        return []

def save_to_csv(data, filename='tn_digital_data.csv'):
    """Save or append data to CSV"""
    if not data:
        return
        
    df = pd.DataFrame(data)
    
    # Check if file exists
    if os.path.exists(filename):
        # Append to existing file without headers
        df.to_csv(filename, mode='a', header=False, index=False)
    else:
        # Create new file with headers
        df.to_csv(filename, index=False)

def get_resume_point(filename):
    """Check existing CSV and determine where to resume scraping"""
    if not os.path.exists(filename):
        return 0, 0
    
    try:
        df = pd.read_csv(filename)
        if len(df) == 0:
            return 0, 0
        
        # Get the highest page_start value and add 100 to continue from next page
        last_page_start = df['page_start'].max()
        next_start = last_page_start + 100
        existing_records = len(df)
        
        print(f"Found existing file with {existing_records} records")
        print(f"Last page was {last_page_start}, resuming from {next_start}")
        
        return next_start, existing_records
        
    except Exception as e:
        print(f"Error reading existing file: {e}")
        return 0, 0

def main():
    filename = 'tn_digital_data.csv'
    
    # Check if we should resume from existing file
    start, total_records = get_resume_point(filename)
    
    if start == 0:
        print("Starting fresh scrape...")
    else:
        print(f"Resuming scrape from page {start}...")
    
    while True:
        print(f"Scraping page starting at {start}...")
        
        # Scrape current page
        page_data = scrape_page(start)
        
        if not page_data:
            print(f"No data found at start={start}. Stopping.")
            break
        
        # Save immediately after each page
        save_to_csv(page_data, filename)
        
        total_records += len(page_data)
        print(f"Saved {len(page_data)} records from page (Total: {total_records})")
        
        # Move to next page
        start += 100
        
        # Wait 2 seconds between requests
        time.sleep(1)
    
    print(f"Scraping complete! Total records: {total_records}")
    print(f"Data saved to: {filename}")

if __name__ == "__main__":
    main()
