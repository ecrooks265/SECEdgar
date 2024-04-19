import sqlite3
import csv

# Connect to the SQLite database
conn = sqlite3.connect('/Databases/company_holdings_2023.db')
cursor = conn.cursor()

# Create the company_holdings table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS top_companies (
        Company TEXT,
        CIK INTEGER,
        Total_Value REAL
    )
''')

# Read data from the TSV file and insert into the company_holdings table
tsv_file = 'top_companies_2023.tsv'
with open(tsv_file, 'r', newline='', encoding='utf-8') as file:
    reader = csv.reader(file, delimiter='\t')
    next(reader)  # Skip the header row
    for row in reader:
        cursor.execute('''
            INSERT INTO top_companies (Company, CIK, Total_Value)
            VALUES (?, ?, ?)
        ''', row)

# Commit the changes and close the connection
conn.commit()
conn.close()

print(f"Data from '{tsv_file}' has been inserted into the top_companies table.")
