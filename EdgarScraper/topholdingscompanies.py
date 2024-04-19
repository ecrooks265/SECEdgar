import sqlite3
import csv
# Creates a TSV file containing the top companies for the year 2023 in total dollar value
# Connect to the database
conn = sqlite3.connect("/Databases/company_holdings_2023.db")
c = conn.cursor()

# SQL query to get the top holding companies by total value for the year 2023
query = '''
    SELECT company_name, cik, SUM(value_usd) AS total_value
    FROM holdings
    WHERE filing_year = '2023'
    GROUP BY company_name
    ORDER BY total_value DESC
    LIMIT 100
'''

# Execute the query
c.execute(query)

# Fetch the results
results = c.fetchall()

# Write the results to a TSV file
output_file = "top_companies_2023.tsv"
with open(output_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file, delimiter="\t")
    writer.writerow(["Company", "CIK", "Total Value (USD)"])
    writer.writerows(results)

print(f"Top companies for 2023 have been written to '{output_file}'.")

# Close the database connection
conn.close()
