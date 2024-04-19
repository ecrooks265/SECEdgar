import sqlite3
import time

def calculate_changes(database_2023, database_2024):
    # Connect to the SQLite databases
    conn_2023 = sqlite3.connect(database_2023)
    conn_2024 = sqlite3.connect(database_2024)
    c_2023 = conn_2023.cursor()
    c_2024 = conn_2024.cursor()

    # List of quarters
    quarters_2023 = [(2023, 1), (2023, 2), (2023, 3), (2023, 4)]
    quarters_2024 = [(2024, 1)]

    # Loop through pairs of consecutive quarters for 2023
    for i in range(len(quarters_2023) - 1):
        current_year, current_quarter = quarters_2023[i]
        next_year, next_quarter = quarters_2023[i + 1]

        # Calculate changes and store in the new table
        calculate_and_store_changes(c_2023, current_year, current_quarter, next_year, next_quarter)

        # Add a sleep timer to slow down the process
        time.sleep(1)  # Sleep for 1 second

    # Loop through pairs of consecutive quarters for 2024
    for i in range(len(quarters_2024) - 1):
        current_year, current_quarter = quarters_2024[i]
        next_year, next_quarter = quarters_2024[i + 1]

        # Calculate changes and store in the new table
        calculate_and_store_changes(c_2024, current_year, current_quarter, next_year, next_quarter)

        # Add a sleep timer to slow down the process
        time.sleep(1)  # Sleep for 1 second

    # Commit changes and close connections
    conn_2023.commit()
    conn_2023.close()
    conn_2024.commit()
    conn_2024.close()

def calculate_and_store_changes(cursor, current_year, current_quarter, next_year, next_quarter):
    # Create a new table for the changes
    changes_table = f"changes_{next_year}_{next_quarter}"
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS {changes_table} (
                        CIK TEXT,
                        HoldingCompanyName TEXT,
                        CompanyName TEXT,
                        Position TEXT,
                        ChangeValue REAL,
                        IsNew BOOLEAN,
                        TotalValueCurrentQuarter REAL,
                        TotalValuePreviousQuarter REAL,
                        PercentageChange REAL
                    )''')

    # Retrieve holdings data for the current and previous quarters
    cursor.execute("SELECT CIK, company_name, name_of_issuer, value_usd FROM holdings WHERE filing_year = ? AND filing_quarter = ?", (current_year, current_quarter))
    current_holdings = cursor.fetchall()
    cursor.execute("SELECT CIK, company_name, name_of_issuer, value_usd FROM holdings WHERE filing_year = ? AND filing_quarter = ?", (next_year, next_quarter))
    next_holdings = cursor.fetchall()

    # Dictionary to store next holdings by CIK and company name
    next_holdings_dict = {(cik, holding_company_name, name_of_issuer): value for cik, holding_company_name, name_of_issuer, value in next_holdings}

    # Calculate total value of the current quarter
    total_value_current_quarter = sum(value for _, _, _, value in current_holdings)

    # Calculate total value of the previous quarter
    total_value_previous_quarter = sum(value for _, _, _, value in next_holdings)

    # Calculate changes and store in the new table
    for cik, holding_company_name, company_name, current_value in current_holdings:
        next_value = next_holdings_dict.get((cik, holding_company_name, company_name), 0)
        change_value = next_value - current_value
        position = 'buy' if change_value > 0 else 'sell'
        is_new = False if next_value else True
        percentage_change = ((next_value - current_value) / current_value) * 100 if current_value != 0 else 0
        cursor.execute(f"INSERT INTO {changes_table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (cik, holding_company_name, company_name, position, change_value, is_new, total_value_current_quarter, total_value_previous_quarter, percentage_change))
    
    print(f"Changes for {current_year} Q{current_quarter} calculated and stored in {changes_table}.")

# Example usage
calculate_changes("/Databases/company_holdings_2023.db", "/Databases/company_holdings_2024.db")
