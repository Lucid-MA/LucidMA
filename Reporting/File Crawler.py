import os
import zipfile
import time

start_time = time.time()

# Base directories to search
base_directories = [
    r"S:\Mandates\Funds\Fund NAV Calculations\USG\USG NAV Packets\End of Month",
    r"S:\Mandates\Funds\Fund NAV Calculations\Prime\Prime NAV Packets\End of Month"
]

# Destination folder base
destination_folder_base = r"S:\Users\THoang\Data\SSC"

# Target filename pattern
target_filename_pattern = "Statement-of-Changes-Period-Detail"

# Current year threshold for old files
year_threshold = 2021

# List to store relevant filenames
relevant_filenames = []

# File to store processed filenames
processed_file_path = os.path.join(destination_folder_base, "Processed file.txt")


# Function to load processed files
def load_processed_files():
    if os.path.exists(processed_file_path):
        with open(processed_file_path, 'r') as file:
            return file.read().splitlines()
    else:
        return []


# Function to save processed files
def save_processed_file(filename):
    with open(processed_file_path, 'a') as file:
        file.write(filename + '\n')


# Load processed files
processed_files = load_processed_files()


# Iterate through base directories
for base_dir in base_directories:
    # Determine postfix based on directory
    postfix = "USG" if "USG" in base_dir else "Prime"

    # Create destination folder with postfix
    destination_folder = os.path.join(destination_folder_base, postfix)
    os.makedirs(destination_folder, exist_ok=True)  # Create if it doesn't exist

    # Get directories matching year threshold
    for year_dir in os.listdir(base_dir):
        if int(year_dir) >= year_threshold:
            year_path = os.path.join(base_dir, year_dir)

            # Process zip files within year directory
            for zip_file in os.listdir(year_path):
                if zip_file.endswith(".zip"):
                    zip_path = os.path.join(year_path, zip_file)

                    # Check if file has been processed
                    if zip_path in processed_files:
                        print(f"File already processed: {zip_path}")
                        continue

                    # Unzip contents
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        for inner_file in zip_ref.namelist():
                            if target_filename_pattern in inner_file:
                                # Extract target file
                                zip_ref.extract(inner_file, destination_folder)

                                # Generate new, post-fixed filename
                                new_filename = inner_file.split('.')[0]
                                new_filename = f"{new_filename}.xlsx"
                                new_filepath = os.path.join(destination_folder, new_filename)

                                # Move file with postfix
                                os.rename(os.path.join(destination_folder, inner_file), new_filepath)
                                new_filepath = os.path.normpath(new_filepath)
                                relevant_filenames.append(new_filepath)

                    # Save processed file
                    save_processed_file(zip_path)

end_time = time.time()
process_time = end_time - start_time
print(f"Processing time: {process_time:.2f} seconds")
