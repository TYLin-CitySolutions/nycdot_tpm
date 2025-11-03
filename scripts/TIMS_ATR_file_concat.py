import pandas as pd
import os

# Define the paths
source_folder = r'C:\Users\dane.rini\Global Infrastructure\SSC 24-01-0670 NYC DOT ESA 21 TPM Project Metrics and Analysis Tool Development - 24-01-0670\09 - Data\TIMS Data\ATR18-23'
output_file = r'C:\Users\dane.rini\Global Infrastructure\SSC 24-01-0670 NYC DOT ESA 21 TPM Project Metrics and Analysis Tool Development - 24-01-0670\09 - Data\TIMS Data\ATR18-23\Combined\atr_df.csv'

# Initialize an empty list to store DataFrames
all_data_frames = []

# Process each file in the source folder
for filename in os.listdir(source_folder):
    if filename.endswith('.xls'):
        filepath = os.path.join(source_folder, filename)
        try:
            # Read header data from cells A1:B15
            header_df = pd.read_excel(filepath, sheet_name=0, header=None, usecols='A:B', nrows=15)
            
            # Extract header data into a dictionary
            header_data = dict(zip(header_df[0], header_df[1]))

            # Read vehicle data from cells A19 onward
            vehicle_df = pd.read_excel(filepath, sheet_name=0, header=None, skiprows=17, usecols='A:C')
            
            # Extract vehicle data column names from the sheet
            vehicle_columns = vehicle_df.iloc[0].tolist()
            vehicle_df.columns = vehicle_columns  # Set column names for vehicle data

            # Drop the first row as it was used for column names
            vehicle_df = vehicle_df.drop(0).reset_index(drop=True)
            
            # Add header data to each row of vehicle data
            for key, value in header_data.items():
                vehicle_df[key] = value

            # Reorder columns to match header_columns order
            header_columns = list(header_data.keys()) + vehicle_columns
            vehicle_df = vehicle_df[header_columns]

            # Add filename for reference
            vehicle_df['Source_File'] = filename

            # Append the DataFrame to the list
            all_data_frames.append(vehicle_df)

        except Exception as e:
            print(f"Error processing file {filepath}: {e}")

# Concatenate all DataFrames and save to CSV
final_df = pd.concat(all_data_frames, ignore_index=True)
final_df.to_csv(output_file, index=False)

