import os
import requests
import xml.etree.ElementTree as ET
import logging
import time
import re

#For testing of logic behind holdingsdbseeder
#Tailor expected output here

# Configure logging
logging.basicConfig(filename='error_log.txt', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s: %(message)s')

# Constants
EDGAR_BASE_URL = "https://www.sec.gov/Archives/"
BATCH_SIZE = 5
RATE_LIMIT_DELAY = 1  # Delay in seconds between batch requests


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
                print(f"{fields[0]} : {fields[1]} : {fields[2]}")
            if len(data) >= 10:  # Limit to the first 10 entries
                break

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
def find_xml_data(txt_link):
    # Set the User-Agent header for the request
    headers = {
        "User-Agent": "google@gmail.com -v1.0"
    }
    xml_contents = []
    # Download the page source using requests with headers
    response = requests.get(txt_link, headers=headers)
    if response.status_code == 200:
        # Split the response text by <XML> and parse each XML content separately
        xml_sections = response.text.split("<XML>")[1:]
        
        for xml in xml_sections:
            print("Extracted XML content:")
            print(xml)

            # Find the start and end positions of the XML section
            start_pos = xml.find("<XML>")
            end_pos = xml.find("</XML>", start_pos)

            if start_pos != -1 and end_pos != -1:
                xml = xml[start_pos:end_pos + 6]
                try:
                    xml_obj = ET.fromstring(xml)  # Parse XML section
                    xml_contents.append(xml_obj)
                except ET.ParseError as e:
                    print(f"XML parsing error: {e}")
            else:
                print("Incomplete XML section found.")
    else:
        print(f"Failed to fetch URL: {txt_link}")

    return xml_contents

def extract_holdings_data(xml_content, company_name, filing_date, company_id):
    # Use regex to find the relevant tags and extract their values
    name_of_issuer = re.findall(r"<nameOfIssuer>(.*?)</nameOfIssuer>", xml_content)
    title_of_class = re.findall(r"<titleOfClass>(.*?)</titleOfClass>", xml_content)
    cusip = re.findall(r"<cusip>(.*?)</cusip>", xml_content)
    value = re.findall(r"<value>(.*?)</value>", xml_content)
    ssh_prnamt = re.findall(r"<sshPrnamt>(.*?)</sshPrnamt>", xml_content)
    ssh_prnamt_type = re.findall(r"<sshPrnamtType>(.*?)</sshPrnamtType>", xml_content)

    if not (name_of_issuer and title_of_class and cusip and value and ssh_prnamt and ssh_prnamt_type):
        return

    # print("===== Company:", company_name, "=====")
    # print("Filing Date:", filing_date)
    # print("CIK:", company_id)

    # # Print the extracted holdings data
    # for i in range(len(name_of_issuer)):
    #     print("----- Holding -----")
    #     print("Name of Issuer:", name_of_issuer[i])
    #     print("Title of Class:", title_of_class[i])
    #     print("CUSIP:", cusip[i])
    #     print("Value:", value[i])
    #     print("Share or Principal Amount:", ssh_prnamt[i], ssh_prnamt_type[i])
    #     print("--------------------")

    # print()


def main():
    directory_path = "./edgar-files/"
    tsv_files = [f for f in os.listdir(directory_path) if f.endswith(".tsv")]

    for tsv_file in tsv_files:
        tsv_file_path = os.path.join(directory_path, tsv_file)
        data = parse_tsv_file(tsv_file_path)
        for entry in data:
            company_id = entry["company_id"]
            company_name = entry["company_name"]
            filing_date = entry["filing_date"]
            txt_link = entry["txt_link"]
            index_link = entry["index_link"]

            print("Fetching XML data for:", company_name)
            xml_data = fetch_xml_data(txt_link)
            if xml_data:
                extract_holdings_data(xml_data, company_name, filing_date, company_id)

            print("--------------------")

if __name__ == "__main__":
    main()
