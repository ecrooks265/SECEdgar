import edgar

# Directory to save the downloaded index files
download_directory = "./edgar-files"

# Since year for downloading the index files (change this to the desired year)
since_year = 2023

# User agent identifier for your script or application
user_agent = "google@gmail.com"

try:
    edgar.download_index(download_directory, since_year, user_agent=user_agent)
    print("Index files downloaded successfully.")
except Exception as e:
    print("Error occurred while downloading index files:", e)
