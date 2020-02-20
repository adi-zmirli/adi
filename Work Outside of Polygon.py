import mysql.connector
import pandas as pd
from app_conf import *

def find_trackings():
    connection = mysql.connector.connect(**conn_big)
    cursor = connection.cursor(buffered=True)

    trackings_query =   """select device_id, IF(driver_code=0,null,driver_code) as driver_code,
                        GROUP_CONCAT(DISTINCT IF(rf_id=0,null,rf_id) SEPARATOR ', ') as attached_implements,
                        from_unixtime(min(timestamp)) as first_timestamp, from_unixtime(max(timestamp)) as last_timestamp,
                        count(*)*10 as total_pto_time
                        from trackings_raw as tr
                        inner join trackings_extra as te on tr.id = te.id
                        where tr.timestamp >= unix_timestamp(date_sub(now(), INTERVAL 24 HOUR))
                        and pto_on = 1
                        and plot_id = 0
                        group by device_id, driver_code
                        having total_pto_time >= 3600
                        order by total_pto_time DESC
                        """

    cursor.execute(trackings_query)

    if cursor.rowcount > 0:
        trackings_df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
    cursor.close()
    connection.close()
    return trackings_df


def find_drivers():
    connection = mysql.connector.connect(**conn)
    cursor = connection.cursor(buffered=True)

    drivers_query = """ select CONCAT(`first_name`, ' ', `last_name`) as driver_name, drivers.code as driver_code
                        from drivers
                        inner join employees
                        on drivers.employee_id = employees.id
                        where drivers.time_to is null
                    """

    cursor.execute(drivers_query)

    if cursor.rowcount > 0:
        drivers_df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
    cursor.close()

    connection.close()
    return drivers_df


trackings_df = find_trackings()
drivers_df = find_drivers()

df = trackings_df.merge(drivers_df, on='driver_code', how='left').drop(['driver_code'], axis=1)
new_df = df.groupby('device_id').apply(lambda x: ','.join(x.driver_name))

print("end")



