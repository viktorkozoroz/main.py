# Instal packedge
import pandas as pd
import numpy as np
import pyodbc as odbc
import sqlalchemy as sql
import psycopg2
import time
from datetime import datetime, date, timedelta

# Login & password
user_hana = ""
pwd_hana = ""
user_pSQL = ""
pwd_pSQL = ""

# function store sale
def store_sale(day_select):
    # get query SQL and save dataframe
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
    HAVING SUM("CC_SALE_GS") >= 1
    ORDER BY "CALDAY" ASC, "PLANT" ASC """.format(dateto=day_select, datefrom=day_select)

    # ---------------------------------------------------------------------
    print("----Connect Hana")
    conn = odbc.connect(DSN="BHP", UID=user_hana, PWD=pwd_hana)
    cursor = conn.cursor()

    df = pd.read_sql(query, conn)

    # disconnect Hana
    cursor.close()
    conn.close()
    # ----------------------------------------------

    # Rename columns
    df.rename(columns={
        'CALDAY': 'date',
        'PLANT': 'store',
        'CC_SALE_PCS_SUM': 'unit_sale',
        'CC_SALE_GS_SUM': 'gross_sale',
        'CC_SALE_NS_SUM': 'net_sale',
        'CC_SALE_COGS_SUM': 'cogs',
        'CC_CHECK_CNT_SUM': 'check',
        'CC_TRAFFIC_TRAF_SUM': 'traffic'}, inplace=True)

    # date format string by format date f"%d-%m-%Y"
    df['date'] = pd.to_datetime(df['date'])

    engine = sql.create_engine('postgresql+psycopg2://postgres:*PSQL-mds-1*!@127.0.0.1:5432/Analytics')
    print("----connect to PostgreSQL")

    # вставка нового массива(df):
    df.to_sql('store_sale', con=engine, schema='Dashboard', if_exists='append', index=False)

    print("----load data to PostgreSQL")

    time.sleep(1)
    print("----ready")


# function store stock
def store_stock(day_select):
    # get query SQL and save dataframe
    query = """SELECT DISTINCT "CALDAY", "PLANT", SUM("ST_COGS") AS "ST_COGS_SUM", SUM("ST_PCS") AS "ST_PCS_SUM"
    FROM "_SYS_BIC"."modis.V_BW/CV_BW2_U_SALES_STOCKS_J_TIME_PRICE"('PLACEHOLDER' = ('$$IP_DATETO$$', '{dateto}'), 'PLACEHOLDER' = ('$$IP_DATEFROM$$', '{datefrom}')) 
    WHERE ("PLANT" NOT IN ('OINV', 'OF01', 'OF02', 'OF03', 'OF04', 'OF05', 'OF06', 'OF07', 'OF08', 'HU01', 'HUB', 'CUST', 'DC03', 'POD', 'DC01'))
    GROUP BY "CALDAY", "PLANT"
    HAVING SUM("ST_PCS") >= 1
    ORDER BY "CALDAY" ASC, "PLANT" ASC""".format(dateto=day_select, datefrom=day_select)

    # Connect Hana
    conn = odbc.connect(DSN="BHP", UID=user_hana, PWD=pwd_hana)
    cursor = conn.cursor()

    df = pd.read_sql(query, conn)

    # disconnect Hana
    cursor.close()
    conn.close()

    # Rename columns
    df.rename(columns={
        'CALDAY': 'date',
        'PLANT': 'store',
        'ST_COGS_SUM': 'cost',
        'ST_PCS_SUM': 'unit_on_stock'}, inplace=True)

    # date format string by format date f"%d-%m-%Y"
    df['date'] = pd.to_datetime(df['date'])

    engine = sql.create_engine('postgresql+psycopg2://postgres:{pwd}@127.0.0.1:5432/{login}').format(pwd=pwd_pSQL, login=user_pSQL)
    print("----store stock - connect to PostgreSQL")

    # вставка нового массива(df):
    df.to_sql('store_stock', con=engine, schema='Dashboard', if_exists='append', index=False)

    print("----store stock - load data to PostgreSQL")

    time.sleep(1)
    print("----store stock - ready")

# start function ETL Sales & Stock store by day
while True:
    counter = pd.Timestamp.today().strftime("%H%M%S")
    if counter == '070000':
        day_select = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
        print("----day_select", day_select)

        store_sale(day_select)

        store_stock(day_select)

