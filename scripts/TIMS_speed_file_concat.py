import os
import pandas as pd
from tkinter import Tk, filedialog

# Prompt user to select a folder
root = Tk()
root.withdraw()  # Hide the root window
folder_path = filedialog.askdirectory(title="Select Folder Containing Excel Files")

if not folder_path:  # Exit if no folder is selected
    print("No folder selected. Exiting...")
    exit()

summary_file = os.path.join(folder_path, "summary.xlsx")

# Initialize an empty list to store summary data
summary_data = []

# Loop through all Excel files in the selected folder
for file in os.listdir(folder_path):
    if file.endswith(".xlsx"):
        file_path = os.path.join(folder_path, file)
        print(f"Processing: {file}")

        try:
            df = pd.read_excel(file_path, header=None)  # Read without headers
            print(f"File {file} loaded successfully with shape {df.shape}")

            # Ensure there are enough rows & columns
            if df.shape[0] < 23 or df.shape[1] < 2:
                print(f"Skipping {file}: Not enough data.")
                continue
            
            # Extract headings from A3:A18 and their corresponding values from B3:B18
            headings = df.iloc[2:18, 0].tolist()  # A3:A18 (0-based index)
            values = df.iloc[2:18, 1].tolist()  # B3:B18
            print(f"Headings extracted: {headings}")
            print(f"Values extracted: {values}")

            # Extract speed data starting from B23 (column 1, row 22 in 0-based index)
            speed_data = df.iloc[22:, 1].dropna().astype(float)  # Drop NaNs and convert to float
            print(f"Speed data extracted: {speed_data.describe()}")  # Print basic stats

            if speed_data.empty:
                print(f"Skipping {file}: No speed data found.")
                continue
            
            # Calculate statistics
            min_speed = speed_data.min()
            max_speed = speed_data.max()
            avg_speed = speed_data.mean()
            p5 = speed_data.quantile(0.05)
            p50 = speed_data.median()
            p95 = speed_data.quantile(0.95)

            # Store the extracted data in a structured format
            summary_data.append([file] + values + [min_speed, max_speed, avg_speed, p5, p50, p95])

        except Exception as e:
            print(f"Error processing {file}: {e}")

# Ensure you have at least one valid entry before saving
if summary_data:
    # Create a df for the summary
    columns = ["File"] + headings + ["Min", "Max", "Average", "5th Percentile", "50th Percentile", "95th Percentile"]
    summary_df = pd.DataFrame(summary_data, columns=columns)

    # Save to an Excel file in the selected folder
    summary_df.to_excel(summary_file, index=False)
    print(f"Summary file '{summary_file}' created successfully!")
else:
    print("No valid data extracted. No summary file created.")
