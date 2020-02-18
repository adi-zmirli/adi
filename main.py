import mysql
import pandas as pd
from app_conf import *


connection = mysql.connector.connect(**conn)
cursor = connection.cursor(buffered=True)

mysql_example = f"select *" \
                f"from device_works"

cursor.execute(mysql_example)

if cursor.rowcount > 0:
    df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
cursor.close()
connection.close()
