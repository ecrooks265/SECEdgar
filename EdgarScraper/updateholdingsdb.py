import os
import re
import sqlite3
import requests
import xml.etree.ElementTree as ET
import logging
import time

# Configure logging
logging.basicConfig(filename='error_log.txt', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s: %(message)s')

# Constants
EDGAR_BASE_URL = "https://www.sec.gov/Archives/"
QUARTER_REGEX = r"(\d{4})-QTR(\d)"
DB_DIRECTORY = "/Databases/"
DB_PREFIX = "company_holdings_"

def parse_tsv_file(file_path):
    data = []
    with open(file_path, "r") as file:
        for line in file:
            fields = line.strip().split("|")
            if "13F-HR" in fields[2]:
                data.append({
                    "company_id": fields[0],
                    "company_name": fields[1],
                    "form_type": fields[2],
                    "filing_date": fields[3],
                    "txt_link": EDGAR_BASE_URL + fields[4],
                    "index_link": EDGAR_BASE_URL + fields[5]
                })
    return data

def fetch_xml_data(xml_url):
    # Set the User-Agent header for the request
    headers = {
        "User-Agent": "google@gmail.com -v1.0"
    }

    try:
        response = requests.get(xml_url, headers=headers)
        response.raise_for_status()  # Raise exception for 4XX and 5XX status codes
        return response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching XML data for URL: {xml_url} - {e}")
        return None

def save_to_database(xml_content, companyId, filing_year, filing_quarter, companyName, filingDate):
    # Connect to SQLite database
    db_file_path = os.path.join(DB_DIRECTORY, f"{DB_PREFIX}{filing_year}.db")
    conn = sqlite3.connect(db_file_path)
    c = conn.cursor()

    # Create a table to store holding data (if not exists)
    c.execute('''
        CREATE TABLE IF NOT EXISTS holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_year TEXT,
            filing_quarter TEXT,
            name_of_issuer TEXT,
            title_of_class TEXT,
            cusip TEXT,
            value_usd TEXT,
            value_usd_thousands TEXT,
            share_amount TEXT,
            share_amount_type TEXT,
            cik TEXT,
            company_name TEXT,
            filing_date TEXT
        )
    ''')

    # Example variables (replace these with actual data)
    cik = companyId
    company_name = companyName
    filing_date = filingDate

    # Use regex to find the relevant tags and extract their values
    name_of_issuer = re.findall(r"<nameOfIssuer>(.*?)</nameOfIssuer>", xml_content)
    title_of_class = re.findall(r"<titleOfClass>(.*?)</titleOfClass>", xml_content)
    cusip = re.findall(r"<cusip>(.*?)</cusip>", xml_content)
    value = re.findall(r"<value>(.*?)</value>", xml_content)
    share_amount = re.findall(r"<sshPrnamt>(.*?)</sshPrnamt>", xml_content)
    share_amount_type = re.findall(r"<sshPrnamtType>(.*?)</sshPrnamtType>", xml_content)

    # Ensure all lists have the same length and are not empty
    if not (name_of_issuer and title_of_class and cusip and value and share_amount and share_amount_type):
        return

    # Determine the unit for the 'value' column (assuming all values in the 'value' column have the same unit)
    value_unit = 'USD' if '$' in value[0] else 'USD_THOUSANDS'

    # Normalize and store values in separate columns based on the unit
    if value_unit == 'USD_THOUSANDS':
        # Convert values to USD by multiplying by 1000
        value_usd = [str(float(val.replace(',', '')) * 1000) for val in value]
        value_usd_thousands = value
    else:
        value_usd = value
        value_usd_thousands = [str(float(val.replace(',', '')) / 1000) for val in value]

    # Insert or update the data into the database for the latest quarter only
    for i in range(len(name_of_issuer)):
        c.execute('''
            INSERT OR REPLACE INTO holdings (filing_year, filing_quarter, name_of_issuer, title_of_class, cusip, value_usd, value_usd_thousands, share_amount, share_amount_type, cik, company_name, filing_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (filing_year, filing_quarter, name_of_issuer[i], title_of_class[i], cusip[i], value_usd[i], value_usd_thousands[i], share_amount[i], share_amount_type[i], cik, company_name, filing_date))

    # Commit the changes and close the database connection
    conn.commit()
    conn.close()

def extract_filing_year_quarter_from_filename(filename):
    # Extract the year and quarter from the file name (assuming the format is "YYYY-QTRX.tsv")
    match = re.match(QUARTER_REGEX, filename)
    if match:
        filing_year = match.group(1)
        filing_quarter = match.group(2)
        return filing_year, filing_quarter
    else:
        return None, None

def main():
    directory_path = "./edgar-files/"
    tsv_files = [f for f in os.listdir(directory_path) if f.endswith(".tsv")]

    latest_year = None
    latest_quarter = None
    latest_tsv_file = None

    for tsv_file in tsv_files:
        filing_year, filing_quarter = extract_filing_year_quarter_from_filename(tsv_file)
        if filing_year and filing_quarter:
            if latest_year is None or (filing_year, filing_quarter) > (latest_year, latest_quarter):
                latest_year = filing_year
                latest_quarter = filing_quarter
                latest_tsv_file = tsv_file

    if latest_year and latest_quarter and latest_tsv_file:
        tsv_file_path = os.path.join(directory_path, latest_tsv_file)
        data = parse_tsv_file(tsv_file_path)

        for entry in data:
            company_id = entry["company_id"]
            company_name = entry["company_name"]
            filing_date = f"{latest_year}-QTR{latest_quarter}"  # Construct filing date
            txt_link = entry["txt_link"]
            index_link = entry["index_link"]

            # Fetch XML data for the company
            xml_data = fetch_xml_data(txt_link)
            if xml_data:
                save_to_database(xml_data, company_id, latest_year, latest_quarter, company_name, filing_date)
    else:
        logging.error("No valid filings found")

if __name__ == "__main__":
    main()
