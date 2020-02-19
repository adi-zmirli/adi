import mysql.connector
import pandas as pd
from app_conf import *

connection = mysql.connector.connect(**conn)
cursor = connection.cursor(buffered=True)

mysql_example = f"select *" \
                f"from device_works " \
                f"limit 50"

cursor.execute(mysql_example)

if cursor.rowcount > 0:
    df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
    print(df)
cursor.close()
connection.close()

