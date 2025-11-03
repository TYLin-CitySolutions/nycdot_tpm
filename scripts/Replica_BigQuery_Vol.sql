SELECT 
    nli, 
    streetname, 
    flags, 
    speed, 
    highway, 
    mode,
    lanes,
    SUM(CASE WHEN start_local_hour = 0 THEN COUNT ELSE 0 END) AS hour_0,
    SUM(CASE WHEN start_local_hour = 1 THEN COUNT ELSE 0 END) AS hour_1,
    SUM(CASE WHEN start_local_hour = 2 THEN COUNT ELSE 0 END) AS hour_2,
    SUM(CASE WHEN start_local_hour = 3 THEN COUNT ELSE 0 END) AS hour_3,
    SUM(CASE WHEN start_local_hour = 4 THEN COUNT ELSE 0 END) AS hour_4,
    SUM(CASE WHEN start_local_hour = 5 THEN COUNT ELSE 0 END) AS hour_5,
    SUM(CASE WHEN start_local_hour = 6 THEN COUNT ELSE 0 END) AS hour_6,
    SUM(CASE WHEN start_local_hour = 7 THEN COUNT ELSE 0 END) AS hour_7,
    SUM(CASE WHEN start_local_hour = 8 THEN COUNT ELSE 0 END) AS hour_8,
    SUM(CASE WHEN start_local_hour = 9 THEN COUNT ELSE 0 END) AS hour_9,
    SUM(CASE WHEN start_local_hour = 10 THEN COUNT ELSE 0 END) AS hour_10,
    SUM(CASE WHEN start_local_hour = 11 THEN COUNT ELSE 0 END) AS hour_11,
    SUM(CASE WHEN start_local_hour = 12 THEN COUNT ELSE 0 END) AS hour_12,
    SUM(CASE WHEN start_local_hour = 13 THEN COUNT ELSE 0 END) AS hour_13,
    SUM(CASE WHEN start_local_hour = 14 THEN COUNT ELSE 0 END) AS hour_14,
    SUM(CASE WHEN start_local_hour = 15 THEN COUNT ELSE 0 END) AS hour_15,
    SUM(CASE WHEN start_local_hour = 16 THEN COUNT ELSE 0 END) AS hour_16,
    SUM(CASE WHEN start_local_hour = 17 THEN COUNT ELSE 0 END) AS hour_17,
    SUM(CASE WHEN start_local_hour = 18 THEN COUNT ELSE 0 END) AS hour_18,
    SUM(CASE WHEN start_local_hour = 19 THEN COUNT ELSE 0 END) AS hour_19,
    SUM(CASE WHEN start_local_hour = 20 THEN COUNT ELSE 0 END) AS hour_20,
    SUM(CASE WHEN start_local_hour = 21 THEN COUNT ELSE 0 END) AS hour_21,
    SUM(CASE WHEN start_local_hour = 22 THEN COUNT ELSE 0 END) AS hour_22,
    SUM(CASE WHEN start_local_hour = 23 THEN COUNT ELSE 0 END) AS hour_23,
    SUM(CASE WHEN start_local_hour BETWEEN 0 AND 23 THEN COUNT ELSE 0 END) AS total_volume
FROM (
    SELECT 
        nli, 
        ns.streetname, 
        ns.flags, 
        ns.speed, 
        ns.highway, 
        ns.lanes, 
        t.start_local_hour,
        t.mode,
        COUNT(*) AS COUNT
    FROM 
        replica-customer.north_atlantic.north_atlantic_2021_Q2_thursday_trip t
    LEFT JOIN 
        UNNEST(network_link_ids) nli
    JOIN 
        replica-customer.north_atlantic.north_atlantic_2021_Q2_network_segments ns
    ON 
        nli = ns.stableedgeid
    WHERE 
        t.mode = '...' -- Mode is filtered here
        AND nli IN ("Enter", "stableedge", "id", "for", "corresponding", "study", "segments", "...") -- Place holder for replica segment_ids
    GROUP BY 
        1,2,3,4,5,6,7,8
) AS hourly_data
GROUP BY 
    1,2,3,4,5,6,7
ORDER BY 
    1,2,3,4,5,6,7;