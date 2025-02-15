import os
import time
import zipfile

from Utils.Common import get_file_path, get_repo_root

"""
This script automates the extraction and organization of financial documents from zip archives 
located within specified directory paths. It is specifically designed for handling "End of Month" 
NAV (Net Asset Value) packets for different funds (USG and Prime). The script performs the following operations:

1. Identifies and processes zip files containing the target file pattern "Statement-of-Changes-Period-Detail"
   within the specified directories for each fund type.
2. Extracts these targeted files and saves them to a structured directory based on the fund type (USG or Prime)
   and renames them to include a unique postfix while converting them into Excel format.
3. Tracks processed files to avoid reprocessing using a text file stored at a specified location.
4. Provides feedback on the processing time and a list of relevant filenames extracted during the session.

Directories Scanned:
- USG NAV Packets under "S:\\Mandates\\Funds\\Fund NAV Calculations\\USG\\USG NAV Packets\\End of Month"
- Prime NAV Packets under "S:\\Mandates\\Funds\\Fund NAV Calculations\\Prime\\Prime NAV Packets\\End of Month"

Output:
- Files are extracted to "S:\\Users\\THoang\\Data\\SSC" under respective subfolders labeled 'USG' and 'Prime'.
- A log of processed files is maintained to ensure files are only processed once.

This script is intended to be run on a regular basis to streamline the management of monthly financial reports and ensure
data consistency and availability for timely financial analysis and reporting.
"""

start_time = time.time()

# Base directories to search
base_directories = [
    r"S:/Mandates/Funds/Fund NAV Calculations/USG/USG NAV Packets/End of Month",
    r"S:/Mandates/Funds/Fund NAV Calculations/Prime/Prime NAV Packets/End of Month",
]

# Destination folder base
destination_folder_base = r"S:/Users/THoang/Data/SSC"

# Target filename pattern
target_filename_pattern = "Statement-of-Changes-Period-Detail"

# Current year threshold for old files
year_threshold = 2021

# List to store relevant filenames
relevant_filenames = []

# Get the repository root directory
repo_path = get_repo_root()
bronze_tracker_dir = repo_path / "Reporting" / "Bronze_tables" / "File_trackers"
processed_file_tracker = bronze_tracker_dir / "Processed SSC Zip files"


def read_processed_files():
    try:
        with open(processed_file_tracker, "r") as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


def mark_file_processed(filename):
    with open(processed_file_tracker, "a") as file:
        file.write(filename + "\n")


# Iterate through base directories
for base_dir in base_directories:
    base_dir = get_file_path(base_dir)
    # Determine postfix based on directory
    postfix = "USG" if "USG" in base_dir else "Prime"

    # Create destination folder with postfix
    destination_folder = get_file_path(os.path.join(destination_folder_base, postfix))
    os.makedirs(destination_folder, exist_ok=True)  # Create if it doesn't exist

    # Get directories matching year threshold
    for year_dir in os.listdir(base_dir):
        if year_dir.isdigit() and int(year_dir) >= year_threshold:
            year_path = os.path.join(base_dir, year_dir)

            # Process zip files within year directory
            for zip_file in os.listdir(year_path):
                if zip_file.endswith(".zip") and zip_file not in read_processed_files():
                    zip_path = os.path.join(year_path, zip_file)

                    # Unzip contents
                    with zipfile.ZipFile(zip_path, "r") as zip_ref:
                        for inner_file in zip_ref.namelist():
                            if target_filename_pattern in inner_file:
                                # Extract target file
                                zip_ref.extract(inner_file, destination_folder)

                                # Generate new, post-fixed filename
                                new_filename = inner_file.split(".")[0]
                                new_filename = f"{new_filename}.xlsx"
                                new_filepath = os.path.join(
                                    destination_folder, new_filename
                                )

                                # Move file with postfix
                                os.rename(
                                    os.path.join(destination_folder, inner_file),
                                    new_filepath,
                                )
                                new_filepath = os.path.normpath(new_filepath)
                                relevant_filenames.append(new_filepath)

                    # Save processed file
                    mark_file_processed(zip_file)

end_time = time.time()
process_time = end_time - start_time
print(f"Processing time: {process_time:.2f} seconds")
