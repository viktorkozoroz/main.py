#Import packedges
import pandas as pd
import pyodbc as odbc
from datetime import datetime, date

today = date.today().strftime("%Y%m%d")

#SQL
date_to = today
date_from = today

#get query SQL and save dataframe
query = """SELECT DISTINCT
"CALDAY", 
"PLANT", 
SUM("CC_SALE_PCS") AS "CC_SALE_PCS_SUM", 
SUM("CC_SALE_GS") AS "CC_SALE_GS_SUM", 
SUM("CC_SALE_NS") AS "CC_SALE_NS_SUM", 
SUM("CC_SALE_COGS") AS "CC_SALE_COGS_SUM", 
SUM("CC_CHECK_CNT") AS "CC_CHECK_CNT_SUM", 
SUM("CC_TRAFFIC_TRAF") AS "CC_TRAFFIC_TRAF_SUM"
FROM "_SYS_BIC"."modis.V_PD/CV_PD2_U_SALES_TRAFFIC"('PLACEHOLDER' = ('$$IP_DATETO$$', '{dateto}'), 'PLACEHOLDER' = ('$$IP_DATEFROM$$', '{datefrom}')) 
GROUP BY "CALDAY", "PLANT"
ORDER BY "CALDAY" ASC, "PLANT" ASC """.format(dateto=date_to, datefrom=date_from)

#Connect Hana
conn = odbc.connect(DSN="BHP", UID="vkozoroz", PWD="*HanaP123*3")
cursor = conn.cursor()

hana_data = pd.read_sql(query, conn)

#disconnect Hana
cursor.close()
conn.close()

#get name columns data frame
hana_data.columns

#Rename columns
hana_data.rename(columns={
    'CALDAY': 'date',
    'PLANT': 'store',
    'CC_SALE_PCS_SUM': 'unit_sale',
    'CC_SALE_GS_SUM': 'gross_sale',
    'CC_SALE_NS_SUM': 'net_sale',
    'CC_SALE_COGS_SUM': 'cogs',
    'CC_CHECK_CNT_SUM': 'check',
    'CC_TRAFFIC_TRAF_SUM': 'traffic'}, inplace = True)

#date format string by format date f"%d-%m-%Y"
hana_data['date'] = hana_data['date'].apply(lambda _: datetime.strptime(_, "%Y%m%d"))

hana_data

