# %%
import os
import pandas as pd
from datetime import datetime
import numpy as np
from datetime import timedelta

# %%
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False) 

# %% [markdown]
# Study segment definition

# %%
atr_segment_ids = pd.read_csv(r'C:\Users\dane.rini\Global Infrastructure\SSC 24-01-0670 NYC DOT ESA 21 TPM Project Metrics and Analysis Tool Development - 24-01-0670\09 - Data\INRIX Data\INRIX_TIMS_segment_data_v2.csv')

# %%
print(atr_segment_ids.columns)

# %%
print(atr_segment_ids['inrix_road_segment_id'].nunique())

# %%
print(atr_segment_ids[atr_segment_ids['seg_id'] == '275380158_0'])

# %% [markdown]
# 2019

# %%
inrix_2019_df_1 = pd.read_csv(r'C:\Users\dane.rini\Global Infrastructure\SSC 24-01-0670 NYC DOT ESA 21 TPM Project Metrics and Analysis Tool Development - 24-01-0670\09 - Data\INRIX Data\atr\2019\INRIX 2019 Volume Profiles (1)\INRIX_OSM_VOLUME_2019_NEWYORK.csv')
print(inrix_2019_df_1.size)

# %%
inrix_2019_df_1.columns = ['seg_id', 'bin_id', 'dow', 'adt', 'volume']

# %%
inrix_2019_df_1['sseg_id'] = inrix_2019_df_1['seg_id'].str.lstrip('-')

# %%
inrix_2019_distinct_segments = inrix_2019_df_1[['seg_id', 'adt']].drop_duplicates()

# %%
inrix_2019_distinct_segments['sseg_id'] = inrix_2019_distinct_segments['seg_id'].str.lstrip('-')

# %%
print(inrix_2019_distinct_segments.head())

# %%
path = r'C:\Users\dane.rini\Global Infrastructure\SSC 24-01-0670 NYC DOT ESA 21 TPM Project Metrics and Analysis Tool Development - 24-01-0670\09 - Data\INRIX Data\segment_reference_merge.csv'
segment_reference = pd.read_csv(path)

# %%
print(segment_reference.head())

# %%
available_segment_data = pd.merge(inrix_2019_distinct_segments, segment_reference, left_on='sseg_id', right_on='seg_id', how='inner')
print(available_segment_data.head())
print(available_segment_data['sseg_id_x'].nunique())


# %%
print(atr_segment_ids['inrix_road_segment_id'].nunique())
print(atr_segment_ids.head())

# %%
segment_coverage_check = pd.merge(atr_segment_ids, available_segment_data, left_on='inrix_road_segment_id', right_on='sseg_id_x', how='inner')
print(segment_coverage_check['inrix_road_segment_id'].nunique())


# %%
# Perform an inner join with atr_segment_ids as the left DataFrame
filtered_df_2019 = pd.merge(atr_segment_ids, inrix_2019_df_1, 
                         left_on='inrix_road_segment_id', 
                         right_on='sseg_id', 
                         how='inner')

# Count the number of rows in the inner join result
inner_join_count = filtered_df_2019.shape[0]

# Display the result and count
print(filtered_df_2019)
print(f"Number of rows in inner join: {inner_join_count}")


# %% [markdown]
# Summing each direction on a given segment

# %%
# Create the `day_type` column
filtered_df_2019['day_type'] = filtered_df_2019['dow'].apply(lambda x: '2: Weekend (Sa-Su)' if x in ['SAT', 'SUN'] else '1: Monday-Friday (M-F)')

# %%
# Step 1: Convert bin_id to time (00:00 + 15 minutes * bin_id)
filtered_df_2019['time'] = filtered_df_2019['bin_id'].apply(lambda x: timedelta(minutes=15) * x)
filtered_df_2019['hour'] = filtered_df_2019['time'].apply(lambda t: t.seconds // 3600)

# Step 2: Average volume per 15-minute bin per day_type
bin_group_cols = [
    'segmentid', 'street', 'lboro', 'xfrom', 'yfrom', 'xto', 'yto',
    'rw_type', 'carto_disp', 'truck_rout', 'seg_id_x', 
    'day_type', 'bin_id', 'time', 'hour', 'dow'
]

combined_dir_df = (
    filtered_df_2019
    .groupby(bin_group_cols, as_index=False)['volume']
    .sum()
)


# %%
print(combined_dir_df.head())

# %% [markdown]
# Averaging by day_type

# %%
# Step 2: Average volume per 15-minute bin per day_type
bin_group_cols = [
    'segmentid', 'street', 'lboro', 'xfrom', 'yfrom', 'xto', 'yto',
    'rw_type', 'carto_disp', 'truck_rout', 'seg_id_x',
    'day_type', 'bin_id', 'time', 'hour'
]

avg_df = (
    combined_dir_df
    .groupby(bin_group_cols, as_index=False)['volume']
    .mean()
)

print(avg_df.head(192))

# %% [markdown]
# Sum by Hour

# %%
# Step 3: Sum avg volumes by hour (already averaged across days)
hour_group_cols = [
    'segmentid', 'street', 'lboro', 'xfrom', 'yfrom', 'xto', 'yto',
    'rw_type', 'carto_disp', 'truck_rout', 'seg_id_x',
    'day_type', 'hour'
]

hourly_df = (
    avg_df
    .groupby(hour_group_cols, as_index=False)['volume']
    .sum()
)
print(hourly_df.head(48))

# %% [markdown]
# Grouping again to get the segment attributes

# %%
print(hourly_df.head())

print(atr_segment_ids.head())

# %%
output_path = r'C:\Users\dane.rini\Global Infrastructure\SSC 24-01-0670 NYC DOT ESA 21 TPM Project Metrics and Analysis Tool Development - 24-01-0670\09 - Data\INRIX Data\atr\2019\inrix_atr_2019_clean_v2.csv'

# Output the DataFrame to CSV
hourly_df.to_csv(output_path, index=False)

print(f"inner_join_df has been saved to {output_path}")

# %% [markdown]
# 2022

# %%
df_inrix_1 = pd.read_parquet(r'C:\Users\dane.rini\Global Infrastructure\SSC 24-01-0670 NYC DOT ESA 21 TPM Project Metrics and Analysis Tool Development - 24-01-0670\09 - Data\INRIX Data\atr\2022\osm-volumeprofilesparquet\INRIX_OSM_VOLUME_2022_NEWYORK.0.snappy.parquet', engine='pyarrow')
df_inrix_2 = pd.read_parquet(r'C:\Users\dane.rini\Global Infrastructure\SSC 24-01-0670 NYC DOT ESA 21 TPM Project Metrics and Analysis Tool Development - 24-01-0670\09 - Data\INRIX Data\atr\2022\osm-volumeprofilesparquet\INRIX_OSM_VOLUME_2022_NEWYORK.1.snappy.parquet', engine='pyarrow')
df_inrix_3 = pd.read_parquet(r'C:\Users\dane.rini\Global Infrastructure\SSC 24-01-0670 NYC DOT ESA 21 TPM Project Metrics and Analysis Tool Development - 24-01-0670\09 - Data\INRIX Data\atr\2022\osm-volumeprofilesparquet\INRIX_OSM_VOLUME_2022_NEWYORK.2.snappy.parquet', engine='pyarrow')

# %%
unioned_df_2022 = pd.concat([df_inrix_1, df_inrix_2, df_inrix_3], ignore_index=True)

print(unioned_df_2022.head(100))

# %%
unioned_df_2022['sseg_id'] = unioned_df_2022['segment_id'].str.lstrip('-')
print(unioned_df_2022.columns)

# %%
path = r'C:\Users\dane.rini\Global Infrastructure\SSC 24-01-0670 NYC DOT ESA 21 TPM Project Metrics and Analysis Tool Development - 24-01-0670\09 - Data\INRIX Data\segment_reference_merge.csv'
segment_reference = pd.read_csv(path)

# %%
print(segment_reference.head())

# %%
available_segment_data = pd.merge(unioned_df_2022, segment_reference, left_on='sseg_id', right_on='seg_id', how='inner')
print(available_segment_data.head())


# %%
print(available_segment_data.sort_values(by='seg_id', ascending=True).head(500))

# %%
print(atr_segment_ids['segmentid'].nunique())
print(atr_segment_ids.size)

# %%
print(atr_segment_ids.head())

# %%
print(unioned_df_2022.head())
print(unioned_df_2022[unioned_df_2022['sseg_id'] == '164752073_0'])

# %%
# Perform an inner join with atr_segment_ids as the left DataFrame
filtered_df_2022 = pd.merge(atr_segment_ids, unioned_df_2022, 
                         left_on='inrix_road_segment_id', 
                         right_on='sseg_id', 
                         how='inner')

# Count the number of rows in the inner join result
inner_join_count = filtered_df_2022.shape[0]

# Display the result and count
print(filtered_df_2022.head())
print(filtered_df_2022[filtered_df_2022['inrix_road_segment_id'] == '5669013_0'])
print(f"Number of rows in inner join: {inner_join_count}")


# %%
filtered_df_2022 = filtered_df_2022[['segmentid', 'street', 'lboro', 'xfrom', 'yfrom', 'xto', 'yto',
                                    'rw_type', 'carto_disp', 'truck_rout', 'seg_id', 'segment_id', 'adt', 'volumes']]

# %%
df = filtered_df_2022.explode('volumes').reset_index(drop=True)

# Add an index column to track array position
df['array_index'] = df.groupby(df.index // (96 * 7)).cumcount()

print(df.head())

# %%
# Map day of the week based on array_index
df['day'] = (df['array_index'] // 96) % 7  # 0 = Sunday, 1 = Monday, ..., 6 = Saturday

# Map the time interval within each day (0 to 95)
df['interval'] = df['array_index'] % 96 


# %%
# Add an hour column (0 to 23)
df['hour'] = df['interval'] // 4
print(df.head())

# %%
day_mapping = {0: 'SUN', 1: 'MON', 2: 'TUE', 3: 'WED',
               4: 'THU', 5: 'FRI', 6: 'SAT'}

df['dow'] = df['day'].map(day_mapping)

# %%
day_type_mapping = {0: '2: Weekend (Sa-Su)', 1: '1: Monday-Friday (M-F)', 2: '1: Monday-Friday (M-F)', 3: '1: Monday-Friday (M-F)', 4: '1: Monday-Friday (M-F)', 5: '1: Monday-Friday (M-F)', 6: '2: Weekend (Sa-Su)'}

df['day_type'] = df['day'].map(day_type_mapping)

# %%
print(df[df['segmentid'] == 9896])

# %%
output_path = r'C:\Users\dane.rini\Global Infrastructure\SSC 24-01-0670 NYC DOT ESA 21 TPM Project Metrics and Analysis Tool Development - 24-01-0670\09 - Data\INRIX Data\atr\2022\inrix_2022_raw.csv'

# Output the DataFrame to CSV
df.to_csv(output_path, index=False)

print(f"inner_join_df has been saved to {output_path}")

# %%
print(df.columns)

# %% [markdown]
# Now that I have filtered to the relevant segments, I want to aggregate the data

# %% [markdown]
# Summing each driving direction on a segment

# %%
bin_group_cols = [
    'segmentid', 'street', 'lboro', 'xfrom', 'yfrom', 'xto', 'yto',
    'rw_type', 'carto_disp', 'truck_rout', 'seg_id', 'array_index', 'day', 'interval', 'hour', 'dow', 'day_type'
]

combined_dir_df = (
    df
    .groupby(bin_group_cols, as_index=False)['volumes']
    .sum()
)

print(combined_dir_df[combined_dir_df['seg_id'] == '5669013_0'].sort_values(by='array_index'))


# %% [markdown]
# Averaging by day_type

# %%
bin_group_cols = [
    'segmentid', 'street', 'lboro', 'xfrom', 'yfrom', 'xto', 'yto',
    'rw_type', 'carto_disp', 'truck_rout', 'seg_id', 'interval', 'hour', 'day_type'
]

day_type_df = (
    combined_dir_df
    .groupby(bin_group_cols, as_index=False)['volumes']
    .mean()
)

print(day_type_df[day_type_df['seg_id'] == '5669013_0'].sort_values(by='interval'))


# %% [markdown]
# Summing hourly values

# %%
bin_group_cols = [
    'segmentid', 'street', 'lboro', 'xfrom', 'yfrom', 'xto', 'yto',
    'rw_type', 'carto_disp', 'truck_rout', 'seg_id', 'hour', 'day_type'
]

hourly_df_2022 = (
    day_type_df
    .groupby(bin_group_cols, as_index=False)['volumes']
    .sum()
)

print(hourly_df_2022[hourly_df_2022['seg_id'] == '5669013_0'].sort_values(by='hour'))


# %%
output_path = r'C:\Users\dane.rini\Global Infrastructure\SSC 24-01-0670 NYC DOT ESA 21 TPM Project Metrics and Analysis Tool Development - 24-01-0670\09 - Data\INRIX Data\atr\2022\inrix_atr_2022_clean_v3.csv'

# Output the DataFrame to CSV
hourly_df_2022.to_csv(output_path, index=False)

print(f"inner_join_df has been saved to {output_path}")


