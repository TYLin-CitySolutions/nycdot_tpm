import os
import pandas as pd
import numpy as np
from collections import defaultdict

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

import pandas as pd
import os
from collections import defaultdict

# Define directory path
directory = r"C:\path\to\vehicle\class\studies\data"

# Storage for data
all_data = []
row_counts = defaultdict(int)  # Tracks number of files with each row count
sheet_counts = defaultdict(int)  # Tracks number of sheets per file

# Process each Excel file
for file in os.listdir(directory):
    if file.endswith(".xlsx"):
        file_path = os.path.join(directory, file)

        # Load the Excel file
        xls = pd.ExcelFile(file_path)
        num_sheets = len(xls.sheet_names)
        sheet_counts[num_sheets] += 1  # Count occurrences of each sheet count

        # Process each sheet in the file
        for sheet_name in xls.sheet_names:
            df = xls.parse(sheet_name=sheet_name, header=None)
            num_rows = df.shape[0]
            row_counts[num_rows] += 1  # Count occurrences of each row length

            # Ensure the file has enough rows
            if num_rows < 115:
                print(f"Skipping {file} - {sheet_name} (only {num_rows} rows, expected at least 115).")
                continue

            # Extract headers (metadata)
            metadata_keys = df.iloc[:14, 0].astype(str).str.lower().tolist() if num_rows >= 14 else []
            metadata_values = df.iloc[:14, 1].tolist() if num_rows >= 14 else []
            metadata_dict = dict(zip(metadata_keys, metadata_values))
            metadata_dict["filename"] = file  # Include filename
            metadata_dict["sheet_name"] = sheet_name  # Include sheet name
            metadata_dict["num_sheets"] = num_sheets  # Track number of sheets

            # Extract column headers for the time-series data (row 18)
            data_headers = df.iloc[18, :10].astype(str).str.lower().tolist() if num_rows >= 18 else []

            # Dynamically adjust columns based on detected headers (starting row 18 in Excel)
            num_cols = min(10, df.shape[1])  # Ensure we don’t exceed available columns

            # Extract data from row 20 onward, without a fixed end limit
            data_section = df.iloc[19:, :num_cols].copy()

            # Assign headers to the data
            if not data_section.empty:
                data_section.columns = data_headers

                # Add metadata to each row
                for _, row in data_section.iterrows():
                    combined_data = metadata_dict.copy()
                    combined_data.update(row.to_dict())
                    all_data.append(combined_data)

# Convert to DataFrame
final_df = pd.DataFrame(all_data)

# Display row count variations
if len(row_counts) > 1:
    print("\n⚠️ Warning: Different row structures detected across files.")
    for i, (num_rows, count) in enumerate(sorted(row_counts.items()), 1):
        print(f"Variation {i}: {count} files have {num_rows} rows.")

# Display sheet count variations
if len(sheet_counts) > 1:
    print("\n⚠️ Warning: Different numbers of sheets detected across files.")
    for i, (num_sheets, count) in enumerate(sorted(sheet_counts.items()), 1):
        print(f"Variation {i}: {count} files have {num_sheets} sheets.")

# Display the first few rows of the final dataset
final_df.head()

final_df.to_csv(r'C:\output\path\vehicle_class_data.csv')