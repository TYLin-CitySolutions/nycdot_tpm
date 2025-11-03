# Initialize a global DataFrame to store all data
global_df = pd.DataFrame()

# Define the full set of expected times
expected_times = pd.date_range("00:00", "23:45", freq="15T").time

def process_sheet(sheet_df, file_name, sheet_name):
    global global_df
    
    header_data = {}
    pedestrian_data_directions = {}
    end_of_header_index = 1

    # Read header data and pedestrian data - finding last row with header data
    for index, row in sheet_df.iterrows():
        if pd.isnull(row[0]) and pd.isnull(row[1]):
            end_of_header_index = index
        elif pd.notnull(row[0]) and index < end_of_header_index:
            print('header row')
            end_of_header_index += 1
            header_name = str(row[0]).strip(':')
            header_value = row[1]
            header_data[header_name] = header_value
        else:
            time = row[0]
            for col in range(1, len(row)):
                try:
                    if pd.notnull(sheet_df.iloc[12, col]):
                        direction = sheet_df.iloc[12, col]
                        if direction not in pedestrian_data_directions:
                            pedestrian_data_directions[direction] = {}
                        pedestrian_data_directions[direction][time] = row[col] if pd.notnull(row[col]) else None
                        print(f"pedestrian data: {pedestrian_data_directions[direction][time]}")
                except Exception as e:
                    print(f"Error processing row {index}, column {col}: {e}")

    # Calculate sums for each time across all directions, initialize with None
    time_sums = {time: None for time in expected_times}
    for direction, data in pedestrian_data_directions.items():
        for time, count in data.items():
            try:
                if count is not None:
                    if time_sums[time] is None:
                        time_sums[time] = 0
                    time_sums[time] += count
            except KeyError as e:
                print(f"KeyError: {e}. Time: {time}. Available times: {list(time_sums.keys())}")

    # Add pedestrian data and sums to header data
    header_data['Pedestrian_Data'] = pedestrian_data_directions
    for time in expected_times:
        header_data[f'Time_{time}_Sum'] = time_sums[time]

    # Add source file name and sheet name to header data
    header_data['Source_File'] = file_name
    header_data['Source_Sheet'] = sheet_name

    # Handle start_date and end_date columns to get day of the week
    if 'Start Date' in header_data:
        header_data['Start Date'] = pd.to_datetime(header_data['Start Date'], errors='coerce')
        header_data['Day_of_Week'] = header_data['Start Date'].day_name() if not pd.isnull(header_data['Start Date']) else None
    else:
        header_data['Day_of_Week'] = None

    # Convert the combined dictionary to a DataFrame with one row
    combined_df = pd.DataFrame([header_data])

    # Append the combined data to the global DataFrame using pd.concat
    global_df = pd.concat([global_df, combined_df], ignore_index=True)
    print(f"Combined DataFrame shape: {combined_df.shape}")
    print(f"Global DataFrame shape after concat: {global_df.shape}")

def process_file(file_path):
    # Read all sheets into a dictionary of DataFrames
    sheets_dict = pd.read_excel(file_path, sheet_name=None, header=None)
    print(f"Processing file: {file_path}")

    # Process each sheet
    for sheet_name, sheet_df in sheets_dict.items():
        print(sheet_name)
        print(sheet_df)
        process_sheet(sheet_df, os.path.basename(file_path), sheet_name)

    print(f"Processed file: {file_path}")

def main():
    directory = r'Path\to\Data'

    if not os.path.isdir(directory):
        print("Invalid directory. Please check the path and try again.")
        return 

    for filename in os.listdir(directory):
        if filename.endswith('.xlsx'):
            file_path = os.path.join(directory, filename)
            process_file(file_path)

df = global_df[['Node ID', 'Segment ID', 'Location1 (On)', 'Location2 (From)', 'Location3 (To)',
       'Borough Code', 'Day_of_Week', 'Start Date', 'Start Time', 'End Date', 'End Time',
       'Interval (min)', 
       'Time_00:00:00_Sum', 'Time_00:15:00_Sum', 'Time_00:30:00_Sum', 'Time_00:45:00_Sum', 'Time_01:00:00_Sum', 'Time_01:15:00_Sum',
       'Time_01:30:00_Sum', 'Time_01:45:00_Sum', 'Time_02:00:00_Sum', 'Time_02:15:00_Sum', 'Time_02:30:00_Sum', 'Time_02:45:00_Sum',
       'Time_03:00:00_Sum', 'Time_03:15:00_Sum', 'Time_03:30:00_Sum', 'Time_03:45:00_Sum', 'Time_04:00:00_Sum', 'Time_04:15:00_Sum',
       'Time_04:30:00_Sum', 'Time_04:45:00_Sum', 'Time_05:00:00_Sum', 'Time_05:15:00_Sum', 'Time_05:30:00_Sum', 'Time_05:45:00_Sum',
       'Time_06:00:00_Sum', 'Time_06:15:00_Sum', 'Time_06:30:00_Sum', 'Time_06:45:00_Sum', 'Time_07:00:00_Sum', 'Time_07:15:00_Sum', 
       'Time_07:30:00_Sum', 'Time_07:45:00_Sum', 'Time_08:00:00_Sum', 'Time_08:15:00_Sum', 'Time_08:30:00_Sum', 'Time_08:45:00_Sum', 
       'Time_09:00:00_Sum', 'Time_09:15:00_Sum', 'Time_09:30:00_Sum', 'Time_09:45:00_Sum', 'Time_10:00:00_Sum', 'Time_10:15:00_Sum', 
       'Time_10:30:00_Sum', 'Time_10:45:00_Sum', 'Time_11:00:00_Sum', 'Time_11:15:00_Sum', 'Time_11:30:00_Sum', 'Time_11:45:00_Sum', 
       'Time_12:00:00_Sum', 'Time_12:15:00_Sum', 'Time_12:30:00_Sum', 'Time_12:45:00_Sum', 'Time_13:00:00_Sum', 'Time_13:15:00_Sum', 
       'Time_13:30:00_Sum', 'Time_13:45:00_Sum', 'Time_14:00:00_Sum', 'Time_14:15:00_Sum', 'Time_14:30:00_Sum', 'Time_14:45:00_Sum', 
       'Time_15:00:00_Sum', 'Time_15:15:00_Sum', 'Time_15:30:00_Sum', 'Time_15:45:00_Sum', 'Time_16:00:00_Sum', 'Time_16:15:00_Sum', 
       'Time_16:30:00_Sum', 'Time_16:45:00_Sum', 'Time_17:00:00_Sum', 'Time_17:15:00_Sum', 'Time_17:30:00_Sum', 'Time_17:45:00_Sum', 
       'Time_18:00:00_Sum', 'Time_18:15:00_Sum', 'Time_18:30:00_Sum', 'Time_18:45:00_Sum', 'Time_19:00:00_Sum', 'Time_19:15:00_Sum', 
       'Time_19:30:00_Sum', 'Time_19:45:00_Sum', 'Time_20:00:00_Sum', 'Time_20:15:00_Sum', 'Time_20:30:00_Sum', 'Time_20:45:00_Sum',
       'Time_21:00:00_Sum', 'Time_21:15:00_Sum', 'Time_21:30:00_Sum', 'Time_21:45:00_Sum', 'Time_22:00:00_Sum', 'Time_22:15:00_Sum',
       'Time_22:30:00_Sum', 'Time_22:45:00_Sum', 'Time_23:00:00_Sum', 'Time_23:15:00_Sum', 'Time_23:30:00_Sum', 'Time_23:45:00_Sum',
       'Pedestrian_Data', 'Source_File', 'Source_Sheet']]

df['Interval (min)'] = 15

df = df.rename(columns={
    'Node ID': 'node_id',
    'Segment ID': 'segment_id', 'Location1 (On)': 'location1_on', 'Location2 (From)': 'location2_from', 'Location3 (To)': 'location3_to', 'Borough Code': 'borough_code',
    'Day_of_Week': 'day_of_week', 'Start Date': 'start_date', 'Start Time': 'start_time', 'End Date': 'end_date', 'End Time': 'end_time', 'Interval (min)': 'interval_min', 
    'Source_File': 'source_file', 'Source_Sheet': 'source_sheet',
    'Time_00:00:00_Sum': '00:00:00_total_ped_count', 'Time_00:15:00_Sum': '00:15:00_total_ped_count', 'Time_00:30:00_Sum': '00:30:00_total_ped_count', 
    'Time_00:45:00_Sum': '00:45:00_total_ped_count', 'Time_01:00:00_Sum': '01:00:00_total_ped_count', 'Time_01:15:00_Sum': '01:15:00_total_ped_count', 
    'Time_01:30:00_Sum': '01:30:00_total_ped_count', 'Time_01:45:00_Sum': '01:45:00_total_ped_count', 'Time_02:00:00_Sum': '02:00:00_total_ped_count', 
    'Time_02:15:00_Sum': '02:15:00_total_ped_count', 'Time_02:30:00_Sum': '02:30:00_total_ped_count', 'Time_02:45:00_Sum': '02:45:00_total_ped_count',
    'Time_03:00:00_Sum': '03:00:00_total_ped_count', 'Time_03:15:00_Sum': '03:15:00_total_ped_count', 'Time_03:30:00_Sum': '03:30:00_total_ped_count', 
    'Time_03:45:00_Sum': '03:45:00_total_ped_count', 'Time_04:00:00_Sum': '04:00:00_total_ped_count', 'Time_04:15:00_Sum': '04:15:00_total_ped_count', 
    'Time_04:30:00_Sum': '04:30:00_total_ped_count', 'Time_04:45:00_Sum': '04:45:00_total_ped_count', 'Time_05:00:00_Sum': '05:00:00_total_ped_count',
    'Time_05:15:00_Sum': '05:15:00_total_ped_count', 'Time_05:30:00_Sum': '05:30:00_total_ped_count', 'Time_05:45:00_Sum': '05:45:00_total_ped_count',
    'Time_06:00:00_Sum': '06:00:00_total_ped_count', 'Time_06:15:00_Sum': '06:15:00_total_ped_count', 'Time_06:30:00_Sum': '06:30:00_total_ped_count', 
    'Time_06:45:00_Sum': '06:45:00_total_ped_count', 'Time_07:00:00_Sum': '07:00:00_total_ped_count', 'Time_07:15:00_Sum': '07:15:00_total_ped_count', 
    'Time_07:30:00_Sum': '07:30:00_total_ped_count', 'Time_07:45:00_Sum': '07:45:00_total_ped_count', 'Time_08:00:00_Sum': '08:00:00_total_ped_count', 
    'Time_08:15:00_Sum': '08:15:00_total_ped_count', 'Time_08:30:00_Sum': '08:30:00_total_ped_count', 'Time_08:45:00_Sum': '08:45:00_total_ped_count',
    'Time_09:00:00_Sum': '09:00:00_total_ped_count', 'Time_09:15:00_Sum': '09:15:00_total_ped_count', 'Time_09:30:00_Sum': '09:30:00_total_ped_count',
    'Time_09:45:00_Sum': '09:45:00_total_ped_count', 'Time_10:00:00_Sum': '10:00:00_total_ped_count', 'Time_10:15:00_Sum': '10:15:00_total_ped_count',
    'Time_10:30:00_Sum': '10:30:00_total_ped_count', 'Time_10:45:00_Sum': '10:45:00_total_ped_count', 'Time_11:00:00_Sum': '11:00:00_total_ped_count',
    'Time_11:15:00_Sum': '11:15:00_total_ped_count', 'Time_11:30:00_Sum': '11:30:00_total_ped_count', 'Time_11:45:00_Sum': '11:45:00_total_ped_count',
    'Time_12:00:00_Sum': '12:00:00_total_ped_count', 'Time_12:15:00_Sum': '12:15:00_total_ped_count', 'Time_12:30:00_Sum': '12:30:00_total_ped_count',
    'Time_12:45:00_Sum': '12:45:00_total_ped_count', 'Time_13:00:00_Sum': '13:00:00_total_ped_count', 'Time_13:15:00_Sum': '13:15:00_total_ped_count',
    'Time_13:30:00_Sum': '13:30:00_total_ped_count', 'Time_13:45:00_Sum': '13:45:00_total_ped_count', 'Time_14:00:00_Sum': '14:00:00_total_ped_count',
    'Time_14:15:00_Sum': '14:15:00_total_ped_count', 'Time_14:30:00_Sum': '14:30:00_total_ped_count', 'Time_14:45:00_Sum': '14:45:00_total_ped_count',
    'Time_15:00:00_Sum': '15:00:00_total_ped_count', 'Time_15:15:00_Sum': '15:15:00_total_ped_count', 'Time_15:30:00_Sum': '15:30:00_total_ped_count',
    'Time_15:45:00_Sum': '15:45:00_total_ped_count', 'Time_16:00:00_Sum': '16:00:00_total_ped_count', 'Time_16:15:00_Sum': '16:15:00_total_ped_count',
    'Time_16:30:00_Sum': '16:30:00_total_ped_count', 'Time_16:45:00_Sum': '16:45:00_total_ped_count', 'Time_17:00:00_Sum': '17:00:00_total_ped_count',
    'Time_17:15:00_Sum': '17:15:00_total_ped_count', 'Time_17:30:00_Sum': '17:30:00_total_ped_count', 'Time_17:45:00_Sum': '17:45:00_total_ped_count',
    'Time_18:00:00_Sum': '18:00:00_total_ped_count', 'Time_18:15:00_Sum': '18:15:00_total_ped_count', 'Time_18:30:00_Sum': '18:30:00_total_ped_count',
    'Time_18:45:00_Sum': '18:45:00_total_ped_count', 'Time_19:00:00_Sum': '19:00:00_total_ped_count', 'Time_19:15:00_Sum': '19:15:00_total_ped_count',
    'Time_19:30:00_Sum': '19:30:00_total_ped_count', 'Time_19:45:00_Sum': '19:45:00_total_ped_count', 'Time_20:00:00_Sum': '20:00:00_total_ped_count',
    'Time_20:15:00_Sum': '20:15:00_total_ped_count', 'Time_20:30:00_Sum': '20:30:00_total_ped_count', 'Time_20:45:00_Sum': '20:45:00_total_ped_count',
    'Time_21:00:00_Sum': '21:00:00_total_ped_count', 'Time_21:15:00_Sum': '21:15:00_total_ped_count', 'Time_21:30:00_Sum': '21:30:00_total_ped_count',
    'Time_21:45:00_Sum': '21:45:00_total_ped_count', 'Time_22:00:00_Sum': '22:00:00_total_ped_count', 'Time_22:15:00_Sum': '22:15:00_total_ped_count',
    'Time_22:30:00_Sum': '22:30:00_total_ped_count', 'Time_22:45:00_Sum': '22:45:00_total_ped_count', 'Time_23:00:00_Sum': '23:00:00_total_ped_count',
    'Time_23:15:00_Sum': '23:15:00_total_ped_count', 'Time_23:30:00_Sum': '23:30:00_total_ped_count', 'Time_23:45:00_Sum': '23:45:00_total_ped_count'
})

# Save the global DataFrame to a CSV file
output_path = r'Output\Path.csv'
df.to_csv(output_path, index=False)

if __name__ == "__main__":
    main()

