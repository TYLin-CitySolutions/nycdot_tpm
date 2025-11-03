import os
import pandas as pd
import numpy as np
from collections import defaultdict
import duckdb

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

sl_class_df = pd.read_csv(r'C:\path\to\combined\streetlight\data\data.csv')
sl_class_df['year'] = sl_class_df['Data Periods'].str.extract(r'(\d{4})').astype(int)
# Assign quarter: If contains 'Jan' -> Q1, else -> Q4
sl_class_df['quarter'] = sl_class_df['Data Periods'].apply(lambda x: 'Q1' if 'Jan' in x else 'Q4')

# filter out all day measurements
sl_class_df = sl_class_df[sl_class_df['Day Type'] != '0: All Days (M-Su)']

sl_class_df = sl_class_df.rename(columns={
"Data Periods":"data_periods",
"Mode of Travel":"mode_of_travel",
"Intersection Type":"intersection_type",
"Zone ID":"zone_id",
"Zone Name":"zone_name",
"Zone Is Pass-Through":"zone_is_pass_through",
"Zone Direction (degrees)":"zone_direction_degrees",
"Zone is Bi-Direction":"zone_is_bi_direction",
"Day Type":"day_type",
"Day Part":"day_part",
"Average Daily Zone Traffic (StL Volume)":"average_daily_zone_traffic_stl_volume",
"Avg Travel Time (sec)":"avg_travel_time_sec",
"Avg All Travel Time (sec)":"avg_all_travel_time_sec",
"Avg Trip Length (mi)":"avg_trip_length_mi",
"Avg All Trip Length (mi)":"avg_all_trip_length_mi"})

sl_class_df = sl_class_df.rename(columns={
"Vehicle Weight":"vehicle_weight"})

query = """
SELECT
    zone_id,
    day_type,
    quarter,
    year,
    vehicle_weight,
    MAX(CASE WHEN day_part = '01: 12am (12am-1am)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "0_tot_vol",
    MAX(CASE WHEN day_part = '02: 1am (1am-2am)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "1_tot_vol",
    MAX(CASE WHEN day_part = '03: 2am (2am-3am)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "2_tot_vol",
    MAX(CASE WHEN day_part = '04: 3am (3am-4am)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "3_tot_vol",
    MAX(CASE WHEN day_part = '05: 4am (4am-5am)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "4_tot_vol",
    MAX(CASE WHEN day_part = '06: 5am (5am-6am)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "5_tot_vol",
    MAX(CASE WHEN day_part = '07: 6am (6am-7am)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "6_tot_vol",
    MAX(CASE WHEN day_part = '08: 7am (7am-8am)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "7_tot_vol",
    MAX(CASE WHEN day_part = '09: 8am (8am-9am)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "8_tot_vol",
    MAX(CASE WHEN day_part = '10: 9am (9am-10am)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "9_tot_vol",
    MAX(CASE WHEN day_part = '11: 10am (10am-11am)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "10_tot_vol",
    MAX(CASE WHEN day_part = '12: 11am (11am-12noon)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "11_tot_vol",
    MAX(CASE WHEN day_part = '13: 12pm (12noon-1pm)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "12_tot_vol",
    MAX(CASE WHEN day_part = '14: 1pm (1pm-2pm)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "13_tot_vol",
    MAX(CASE WHEN day_part = '15: 2pm (2pm-3pm)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "14_tot_vol",
    MAX(CASE WHEN day_part = '16: 3pm (3pm-4pm)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "15_tot_vol",
    MAX(CASE WHEN day_part = '17: 4pm (4pm-5pm)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "16_tot_vol",
    MAX(CASE WHEN day_part = '18: 5pm (5pm-6pm)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "17_tot_vol",
    MAX(CASE WHEN day_part = '19: 6pm (6pm-7pm)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "18_tot_vol",
    MAX(CASE WHEN day_part = '20: 7pm (7pm-8pm)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "19_tot_vol",
    MAX(CASE WHEN day_part = '21: 8pm (8pm-9pm)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "20_tot_vol",
    MAX(CASE WHEN day_part = '22: 9pm (9pm-10pm)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "21_tot_vol",
    MAX(CASE WHEN day_part = '23: 10pm (10pm-11pm)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "22_tot_vol",
    MAX(CASE WHEN day_part = '24: 11pm (11pm-12am)' THEN average_daily_zone_traffic_stl_volume ELSE 0 END) AS "23_tot_vol"
FROM "24-01-0670_NYCDOT-ESA".sl_class_df
WHERE day_type != '0: All Days (M-Su)' 
GROUP BY zone_id, day_type, quarter, year, vehicle_weight
ORDER BY zone_id, day_type, quarter;
"""

sl_class_df_piv = duckdb.query(query).fetchall()

sl_class_df_piv_df = pd.DataFrame(sl_class_df_piv, columns=["zone_id", "day_type", "quarter", "year", "vehicle_weight","0_tot_vol", "1_tot_vol", "2_tot_vol", "3_tot_vol", "4_tot_vol", 
                                                        "5_tot_vol", "6_tot_vol", "7_tot_vol", "8_tot_vol", "9_tot_vol", "10_tot_vol", "11_tot_vol", "12_tot_vol", "13_tot_vol", "14_tot_vol",
                                                        "15_tot_vol", "16_tot_vol", "17_tot_vol", "18_tot_vol", "19_tot_vol", "20_tot_vol", "21_tot_vol", "22_tot_vol", "23_tot_vol"])

print(sl_class_df_piv_df)

sl_class_df_piv_df.to_csv(r'C:\path\to\pivoted\output\of\SL\data\data.csv')