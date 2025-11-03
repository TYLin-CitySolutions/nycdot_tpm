import os
import numpy as np
import pandas as pd

# get current work folder
current_directory = os.getcwd()

# define source and target folder
source_folder = os.path.join(os.path.dirname(current_directory), 'All Data', 'Male Female Format')
processed_folder = os.path.join(os.path.dirname(current_directory), 'All Data', 'Processed Data', 'Male Female Format')

# if folder doesn't exsit, create one
os.makedirs(processed_folder, exist_ok=True)

# get all excel files
excel_files = [f for f in os.listdir(source_folder) if f.endswith(('.xlsx','.XLSX'))]

# choose the first excel file
first_excel_file = excel_files[0]
first_excel_file_path = os.path.join(source_folder, first_excel_file)

# read first excel file first sheet
df = pd.read_excel(first_excel_file_path, sheet_name=0, header=None)

# get first column first 95 row values
first_column = df.iloc[:95, 0]

# exclude useless columns
exclude_rows = [0, 14, 15, 16, 17, 18, 19, 20, 21, 22]
filtered_column_names = [value for idx, value in enumerate(first_column) if idx not in exclude_rows]

# frame a result df with first excel file first sheet first column
bike_data = pd.DataFrame(columns=filtered_column_names+ ['file_name', 'sheet_number'])

# define a function to calculate sum for each row
def calculate_row_sum(row):
    if row.isnull().all():
        return None  # if all NA, then sum = NA
    return row.fillna(0).sum()  # if not all NA, see NA as 0, then sum

# process first 2 sheets for every excel files
for file_name in excel_files:
    file_path = os.path.join(source_folder, file_name)
    
    for sheet_index in range(2):  # loop first 2 sheets
        df = pd.read_excel(file_path, sheet_name=sheet_index, header=None)
        
        # get numbers
        sub_df = df.iloc[23:95, 1:11]
        sub_df = sub_df.apply(pd.to_numeric, errors='coerce')
        
        # calculate row sum
        row_sums = sub_df.apply(calculate_row_sum, axis=1)

        # get information values of this sheet
        info_values = df.iloc[1:14, 1].values

        # create a new dataframe row to store info_values and row_sum
        new_row = pd.Series([None] * len(bike_data.columns), index=bike_data.columns)
       
        # put info_values in new_row
        for i in range(len(info_values)):
            new_row.iloc[i] = info_values[i]
        
        # put row_sum in new_row
        for idx, value in enumerate(row_sums.values):
            column_index = 13 + idx  # put in right place (from 14th column)
            if column_index < len(bike_data.columns):
                new_row.iloc[column_index] = value

        # add file_name and sheet_index
        new_row['file_name'] = file_name
        new_row['sheet_number'] = sheet_index + 1
        
        # add new row to result df
        bike_data = pd.concat([bike_data, pd.DataFrame([new_row])], ignore_index=True)

bike_data = bike_data.rename(columns={
    bike_data.columns[0]: 'segment_id', bike_data.columns[1]: 'station_id', bike_data.columns[2]: 'location1_on', bike_data.columns[3]: 'location2_from', bike_data.columns[4]: 'location3_to', 
    bike_data.columns[5]: 'direction', bike_data.columns[6]: 'FHWA_code', bike_data.columns[7]: 'borough_code', bike_data.columns[8]: 'start_date', bike_data.columns[9]: 'start_time', 
    bike_data.columns[10]: 'end_date', bike_data.columns[11]: 'end_time', bike_data.columns[12]: 'interval_min', 
    bike_data.columns[13]: '06:00_total_bike_count', bike_data.columns[14]: '06:15_total_bike_count', bike_data.columns[15]: '06:30_total_bike_count', bike_data.columns[16]: '06:45_total_bike_count', 
    bike_data.columns[17]: '07:00_total_bike_count', bike_data.columns[18]: '07:15_total_bike_count', bike_data.columns[19]: '07:30_total_bike_count', bike_data.columns[20]: '07:45_total_bike_count',
    bike_data.columns[21]: '08:00_total_bike_count', bike_data.columns[22]: '08:15_total_bike_count', bike_data.columns[23]: '08:30_total_bike_count', bike_data.columns[24]: '08:45_total_bike_count', 
    bike_data.columns[25]: '09:00_total_bike_count', bike_data.columns[26]: '09:15_total_bike_count', bike_data.columns[27]: '09:30_total_bike_count', bike_data.columns[28]: '09:45_total_bike_count', 
    bike_data.columns[29]: '10:00_total_bike_count', bike_data.columns[30]: '10:15_total_bike_count', bike_data.columns[31]: '10:30_total_bike_count', bike_data.columns[32]: '10:45_total_bike_count',
    bike_data.columns[33]: '11:00_total_bike_count', bike_data.columns[34]: '11:15_total_bike_count', bike_data.columns[35]: '11:30_total_bike_count', bike_data.columns[36]: '11:45_total_bike_count', 
    bike_data.columns[37]: '12:00_total_bike_count', bike_data.columns[38]: '12:15_total_bike_count', bike_data.columns[39]: '12:30_total_bike_count', bike_data.columns[40]: '12:45_total_bike_count', 
    bike_data.columns[41]: '13:00_total_bike_count', bike_data.columns[42]: '13:15_total_bike_count', bike_data.columns[43]: '13:30_total_bike_count', bike_data.columns[44]: '13:45_total_bike_count',
    bike_data.columns[45]: '14:00_total_bike_count', bike_data.columns[46]: '14:15_total_bike_count', bike_data.columns[47]: '14:30_total_bike_count', bike_data.columns[48]: '14:45_total_bike_count', 
    bike_data.columns[49]: '15:00_total_bike_count', bike_data.columns[50]: '15:15_total_bike_count', bike_data.columns[51]: '15:30_total_bike_count', bike_data.columns[52]: '15:45_total_bike_count', 
    bike_data.columns[53]: '16:00_total_bike_count', bike_data.columns[54]: '16:15_total_bike_count', bike_data.columns[55]: '16:30_total_bike_count', bike_data.columns[56]: '16:45_total_bike_count',
    bike_data.columns[57]: '17:00_total_bike_count', bike_data.columns[58]: '17:15_total_bike_count', bike_data.columns[59]: '17:30_total_bike_count', bike_data.columns[60]: '17:45_total_bike_count', 
    bike_data.columns[61]: '18:00_total_bike_count', bike_data.columns[62]: '18:15_total_bike_count', bike_data.columns[63]: '18:30_total_bike_count', bike_data.columns[64]: '18:45_total_bike_count', 
    bike_data.columns[65]: '19:00_total_bike_count', bike_data.columns[66]: '19:15_total_bike_count', bike_data.columns[67]: '19:30_total_bike_count', bike_data.columns[68]: '19:45_total_bike_count',
    bike_data.columns[69]: '20:00_total_bike_count', bike_data.columns[70]: '20:15_total_bike_count', bike_data.columns[71]: '20:30_total_bike_count', bike_data.columns[72]: '20:45_total_bike_count',
    bike_data.columns[73]: '21:00_total_bike_count', bike_data.columns[74]: '21:15_total_bike_count', bike_data.columns[75]: '21:30_total_bike_count', bike_data.columns[76]: '21:45_total_bike_count',
    bike_data.columns[77]: '22:00_total_bike_count', bike_data.columns[78]: '22:15_total_bike_count', bike_data.columns[79]: '22:30_total_bike_count', bike_data.columns[80]: '22:45_total_bike_count',
    bike_data.columns[81]: '23:00_total_bike_count', bike_data.columns[82]: '23:15_total_bike_count', bike_data.columns[83]: '23:30_total_bike_count', bike_data.columns[84]: '23:45_total_bike_count',
})

# set pandas display options to show all columns
pd.set_option('display.max_columns', None)

bike_data.head(5)

bike_data.shape

# clean start and end time column

# make them into time format
bike_data['start_time'] = pd.to_datetime(bike_data['start_time'], format='%I:%M%p', errors='coerce').combine_first(pd.to_datetime(bike_data['start_time'], format='%H:%M:%S', errors='coerce'))
bike_data['end_time'] = pd.to_datetime(bike_data['end_time'], format='%I:%M%p', errors='coerce').combine_first(pd.to_datetime(bike_data['end_time'], format='%H:%M:%S', errors='coerce'))

# define function to add 12 hours if end time is too small (should not be smaller than 10:00 am) & solve 00:00 AM end time error
def end_time_error(t):
    if pd.isna(t):
        return t
    dt = pd.to_datetime(t.strftime('%H:%M:%S'), format='%H:%M:%S')
    # If end time is smaller than 10:00 AM, add 12 hours
    if dt.time() < pd.to_datetime('10:00:00', format='%H:%M:%S').time():
        dt += pd.Timedelta(hours=12)
    # If end time is 12:00 PM, change it to 23:59:00
    if dt.time() == pd.to_datetime('12:00:00', format='%H:%M:%S').time():
        dt = pd.to_datetime('23:59:00', format='%H:%M:%S')
    return dt.time()

bike_data['end_time'] = bike_data['end_time'].apply(end_time_error)

bike_data.head(5)

bike_data.shape

# delete invalid values in each row
bike_data['start_time'] = pd.to_datetime(bike_data['start_time'], format='%H:%M:%S').dt.time
bike_data['end_time'] = pd.to_datetime(bike_data['end_time'], format='%H:%M:%S').dt.time

# select time columns
time_columns = bike_data.columns[13:85]

# define function to clean time columns
def compare_and_assign(row):
    start_time = row['start_time']
    end_time = row['end_time']

    # if no start_time or end_time or end_time = 00:00:00, skip function
    if pd.isna(start_time) or pd.isna(end_time) or end_time == pd.to_datetime('00:00:00', format='%H:%M:%S').time():
        return row
        
    for col in time_columns:
        # grab time from column name
        col_time = pd.to_datetime(col.split('_')[0], format='%H:%M').time()
        # compare start time/end time and column time
        if col_time < start_time:
            row[col] = np.nan
        if col_time >= end_time:
            row[col] = np.nan
    return row

# apply function
bike_data = bike_data.apply(compare_and_assign, axis=1)

bike_data.head(5)

# remove "all NA" rows
cols_to_check = bike_data.columns[13:85]
bike_data = bike_data.dropna(subset=cols_to_check, how='all')

bike_data = bike_data.reset_index(drop=True)

bike_data.shape

# subtle change for each column
bike_data['interval_min'] = bike_data['interval_min'].replace({'15 Mins': '15', '15 min': '15', '15': '15'}, regex=True)
bike_data['start_date'] = pd.to_datetime(bike_data['start_date'], errors='coerce').dt.strftime('%Y-%m-%d')
bike_data['end_date'] = pd.to_datetime(bike_data['end_date'], errors='coerce').dt.strftime('%Y-%m-%d')
bike_data.iloc[:, :6] = bike_data.iloc[:, :6].replace(0, np.nan)

bike_data.head(5)

output_file = os.path.join(processed_folder, f'Processed_{pd.Timestamp.now().strftime("%Y-%m-%d")}.csv')
bike_data.to_csv(output_file, index=False)

