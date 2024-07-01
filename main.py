from fastapi import FastAPI
import uvicorn
import re
import pandas as pd
import pyodbc
import pypyodbc as odbc
import os
import json 
import re

app = FastAPI()

conn_str="Driver={ODBC Driver 18 for SQL Server};Server=tcp:ukcotestserver.database.windows.net,1433;Database=ukcotestdb;Uid=Saif;Pwd=Ukcotest@;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

@app.get("/sale_prevday/")
async def sale_prevday(states: str | None = None):
    if states:
        def remove_comma_before_parenthesis(text):
                # Regex pattern to find a comma before a closing parenthesis
                pattern = r',(?=\))'
                # Substitute the comma with an empty string
                result = re.sub(pattern, '', text)
                return result
        l_states=states.split(',')
        l_states=[i.strip() for i in l_states]
        str_states=str(tuple(l_states))
        str_states=remove_comma_before_parenthesis(str_states)
        with pyodbc.connect(conn_str) as conn1:
            query1 = '''
            SELECT Naturo.sales_tvisit.visit_id, Naturo.sales_tvisit.outlet_guid, Naturo.sales_tvisit.time, Naturo.sales_tvisit.product_division, Naturo.sales_tvisit.product_name, Naturo.sales_tvisit.quantity, Naturo.sales_tvisit.price, Naturo.sales_tvisit.quantity*Naturo.sales_tvisit.price as total_sales, Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name FROM Naturo.sales_tvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_tvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_tvisit.time BETWEEN (SELECT CAST(DATEADD(DAY,-1,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit) AND Naturo.moutlet4.region IN {};
                    '''.format(str_states)
        df = pd.read_sql(sql=query1, con=conn1)
        
    else:
        with pyodbc.connect(conn_str) as conn1:
            query1 = '''
            SELECT Naturo.sales_tvisit.visit_id, Naturo.sales_tvisit.outlet_guid, Naturo.sales_tvisit.time, Naturo.sales_tvisit.product_division, Naturo.sales_tvisit.product_name, Naturo.sales_tvisit.quantity, Naturo.sales_tvisit.price, Naturo.sales_tvisit.quantity*Naturo.sales_tvisit.price as total_sales, Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name FROM Naturo.sales_tvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_tvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_tvisit.time BETWEEN (SELECT CAST(DATEADD(DAY,-1,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit);
                    '''
        df = pd.read_sql(sql=query1, con=conn1)


    return {"main_tb":df.to_dict(orient="records")}
  

@app.get("/sale_prevday/")
async def state_sale_prevday(states: str | None = None):
    if states:
        def remove_comma_before_parenthesis(text):
                # Regex pattern to find a comma before a closing parenthesis
                pattern = r',(?=\))'
                # Substitute the comma with an empty string
                result = re.sub(pattern, '', text)
                return result
        l_states=states.split(',')
        l_states=[i.strip() for i in l_states]
        str_states=str(tuple(l_states))
        str_states=remove_comma_before_parenthesis(str_states)
        with pyodbc.connect(conn_str) as conn1:
            query1 = '''
            SELECT Naturo.sales_tvisit.visit_id, Naturo.sales_tvisit.outlet_guid, Naturo.sales_tvisit.time, Naturo.sales_tvisit.product_division, Naturo.sales_tvisit.product_name, Naturo.sales_tvisit.quantity, Naturo.sales_tvisit.price, Naturo.sales_tvisit.quantity*Naturo.sales_tvisit.price as total_sales, Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name FROM Naturo.sales_tvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_tvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_tvisit.time BETWEEN (SELECT CAST(DATEADD(DAY,-1,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit) AND Naturo.moutlet4.region IN {};
                    '''.format(str_states)
        df = pd.read_sql(sql=query1, con=conn1)
        
    else:
        with pyodbc.connect(conn_str) as conn1:
            query1 = '''
            SELECT Naturo.sales_tvisit.visit_id, Naturo.sales_tvisit.outlet_guid, Naturo.sales_tvisit.time, Naturo.sales_tvisit.product_division, Naturo.sales_tvisit.product_name, Naturo.sales_tvisit.quantity, Naturo.sales_tvisit.price, Naturo.sales_tvisit.quantity*Naturo.sales_tvisit.price as total_sales, Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name FROM Naturo.sales_tvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_tvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_tvisit.time BETWEEN (SELECT CAST(DATEADD(DAY,-1,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit);
                    '''
        df = pd.read_sql(sql=query1, con=conn1)


    return {"main_tb":df.to_dict(orient="records")}


@app.get("/sale_week/")
async def sale_week(states: str | None = None):
    if states:
        def remove_comma_before_parenthesis(text):
                # Regex pattern to find a comma before a closing parenthesis
                pattern = r',(?=\))'
                # Substitute the comma with an empty string
                result = re.sub(pattern, '', text)
                return result
        l_states=states.split(',')
        l_states=[i.strip() for i in l_states]
        str_states=str(tuple(l_states))
        str_states=remove_comma_before_parenthesis(str_states)
        with pyodbc.connect(conn_str) as conn1:
            query1 = '''
            SELECT Naturo.sales_tvisit.visit_id, Naturo.sales_tvisit.outlet_guid, Naturo.sales_tvisit.time, Naturo.sales_tvisit.product_division, Naturo.sales_tvisit.product_name, Naturo.sales_tvisit.quantity, Naturo.sales_tvisit.price, Naturo.sales_tvisit.quantity*Naturo.sales_tvisit.price as total_sales, Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name FROM Naturo.sales_tvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_tvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_tvisit.time > (SELECT CAST(DATEADD(WEEK,-2,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and Naturo.sales_tvisit.time<= (SELECT MAX(time) FROM Naturo.sales_mvisit) AND Naturo.moutlet4.region IN {};
                    '''.format(str_states)
        df = pd.read_sql(sql=query1, con=conn1)
        
    else:
        with pyodbc.connect(conn_str) as conn1:
            query1 = '''
            SELECT Naturo.sales_tvisit.visit_id, Naturo.sales_tvisit.outlet_guid, Naturo.sales_tvisit.time, Naturo.sales_tvisit.product_division, Naturo.sales_tvisit.product_name, Naturo.sales_tvisit.quantity, Naturo.sales_tvisit.price, Naturo.sales_tvisit.quantity*Naturo.sales_tvisit.price as total_sales, Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name FROM Naturo.sales_tvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_tvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_tvisit.time > (SELECT CAST(DATEADD(WEEK,-2,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and Naturo.sales_tvisit.time<= (SELECT MAX(time) FROM Naturo.sales_mvisit);
                    '''
        df = pd.read_sql(sql=query1, con=conn1)


    return {"main_tb":df.to_dict(orient="records")}

@app.get("/best_per_outlets/")
async def best_per_outlets(states: str | None = None):
    if states:
        def remove_comma_before_parenthesis(text):
                # Regex pattern to find a comma before a closing parenthesis
                pattern = r',(?=\))'
                # Substitute the comma with an empty string
                result = re.sub(pattern, '', text)
                return result
        l_states=states.split(',')
        l_states=[i.strip() for i in l_states]
        str_states=str(tuple(l_states))
        str_states=remove_comma_before_parenthesis(str_states)
        with pyodbc.connect(conn_str) as conn1:
            query1 = '''
            WITH tc_tb AS
            (SELECT outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name, SUM(order_qty) as qty, SUM(order_value) as sale, COUNT(outlet_guid) as is_repeat FROM
            (SELECT Naturo.sales_mvisit.visit_id, Naturo.sales_mvisit.is_productive, Naturo.sales_mvisit.outlet_guid, Naturo.moutlet4.outlet_type, Naturo.moutlet4.outlet_name, Naturo.moutlet4.latitude,Naturo.moutlet4.longitude, Naturo.moutlet4.created_at,Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name, Naturo.sales_mvisit.order_qty, Naturo.sales_mvisit.order_value FROM Naturo.sales_mvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_mvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_mvisit.time BETWEEN (SELECT CAST(DATEADD(MONTH,-6,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit) AND Naturo.moutlet4.created_at NOT BETWEEN (SELECT CAST(DATEADD(MONTH,-3,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit)) AS tb
            GROUP BY outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name),

            pc_tb AS
            (SELECT outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name, SUM(order_qty) as qty, SUM(order_value) as sale, COUNT(outlet_guid) as is_repeat_productive FROM
            (SELECT Naturo.sales_mvisit.visit_id, Naturo.sales_mvisit.is_productive, Naturo.sales_mvisit.outlet_guid, Naturo.moutlet4.outlet_type, Naturo.moutlet4.outlet_name, Naturo.moutlet4.latitude,Naturo.moutlet4.longitude, Naturo.moutlet4.created_at,Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name, Naturo.sales_mvisit.order_qty, Naturo.sales_mvisit.order_value FROM Naturo.sales_mvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_mvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_mvisit.time BETWEEN (SELECT CAST(DATEADD(MONTH,-6,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit) AND Naturo.sales_mvisit.is_productive=1 AND Naturo.moutlet4.created_at NOT BETWEEN (SELECT CAST(DATEADD(MONTH,-3,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit)) AS tb
            GROUP BY outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name)

            SELECT
                tc_tb.outlet_guid,
                tc_tb.outlet_type,
                tc_tb.outlet_name,
                tc_tb.latitude,
                tc_tb.longitude,
                tc_tb.created_at,
                tc_tb.region,
                tc_tb.territory,
                tc_tb.beat_name,
                tc_tb.qty,
                tc_tb.sale,
                tc_tb.is_repeat,
                COALESCE(pc_tb.is_repeat_productive, 0) AS is_repeat_productive
            FROM tc_tb
            LEFT JOIN pc_tb ON tc_tb.outlet_guid=pc_tb.outlet_guid
            WHERE tc_tb.is_repeat>=6 AND pc_tb.is_repeat_productive>=6 AND tc_tb.region IN {};
                    '''.format(str_states)
        df = pd.read_sql(sql=query1, con=conn1)
        df['productive_rate']=df['is_repeat_productive']/df['is_repeat']
        df.sort_values(by=["productive_rate","is_repeat_productive"],ascending=False,inplace=True)
        
    else:
        with pyodbc.connect(conn_str) as conn1:
            query1 = '''
            WITH tc_tb AS
            (SELECT outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name, SUM(order_qty) as qty, SUM(order_value) as sale, COUNT(outlet_guid) as is_repeat FROM
            (SELECT Naturo.sales_mvisit.visit_id, Naturo.sales_mvisit.is_productive, Naturo.sales_mvisit.outlet_guid, Naturo.moutlet4.outlet_type, Naturo.moutlet4.outlet_name, Naturo.moutlet4.latitude,Naturo.moutlet4.longitude, Naturo.moutlet4.created_at,Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name, Naturo.sales_mvisit.order_qty, Naturo.sales_mvisit.order_value FROM Naturo.sales_mvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_mvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_mvisit.time BETWEEN (SELECT CAST(DATEADD(MONTH,-6,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit) AND Naturo.moutlet4.created_at NOT BETWEEN (SELECT CAST(DATEADD(MONTH,-3,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit)) AS tb
            GROUP BY outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name),

            pc_tb AS
            (SELECT outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name, SUM(order_qty) as qty, SUM(order_value) as sale, COUNT(outlet_guid) as is_repeat_productive FROM
            (SELECT Naturo.sales_mvisit.visit_id, Naturo.sales_mvisit.is_productive, Naturo.sales_mvisit.outlet_guid, Naturo.moutlet4.outlet_type, Naturo.moutlet4.outlet_name, Naturo.moutlet4.latitude,Naturo.moutlet4.longitude, Naturo.moutlet4.created_at,Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name, Naturo.sales_mvisit.order_qty, Naturo.sales_mvisit.order_value FROM Naturo.sales_mvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_mvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_mvisit.time BETWEEN (SELECT CAST(DATEADD(MONTH,-6,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit) AND Naturo.sales_mvisit.is_productive=1 AND Naturo.moutlet4.created_at NOT BETWEEN (SELECT CAST(DATEADD(MONTH,-3,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit)) AS tb
            GROUP BY outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name)

            SELECT
                tc_tb.outlet_guid,
                tc_tb.outlet_type,
                tc_tb.outlet_name,
                tc_tb.latitude,
                tc_tb.longitude,
                tc_tb.created_at,
                tc_tb.region,
                tc_tb.territory,
                tc_tb.beat_name,
                tc_tb.qty,
                tc_tb.sale,
                tc_tb.is_repeat,
                COALESCE(pc_tb.is_repeat_productive, 0) AS is_repeat_productive
            FROM tc_tb
            LEFT JOIN pc_tb ON tc_tb.outlet_guid=pc_tb.outlet_guid
            WHERE tc_tb.is_repeat>=6 AND pc_tb.is_repeat_productive>=6;
                    '''
        df = pd.read_sql(sql=query1, con=conn1)
        df['productive_rate']=df['is_repeat_productive']/df['is_repeat']
        df.sort_values(by=["productive_rate","is_repeat_productive"],ascending=False,inplace=True)

    return {"main_tb":df.reset_index(drop=True).to_dict(orient="records")}


@app.get("/under_per_outlets/")
async def under_per_outlets(states: str | None = None):
    if states:
        def remove_comma_before_parenthesis(text):
                # Regex pattern to find a comma before a closing parenthesis
                pattern = r',(?=\))'
                # Substitute the comma with an empty string
                result = re.sub(pattern, '', text)
                return result
        l_states=states.split(',')
        l_states=[i.strip() for i in l_states]
        str_states=str(tuple(l_states))
        str_states=remove_comma_before_parenthesis(str_states)
        with pyodbc.connect(conn_str) as conn1:
            query1 = '''
           WITH tc_tb AS
            (SELECT outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name, SUM(order_qty) as qty, SUM(order_value) as sale, COUNT(outlet_guid) as is_repeat FROM
            (SELECT Naturo.sales_mvisit.visit_id, Naturo.sales_mvisit.is_productive, Naturo.sales_mvisit.outlet_guid, Naturo.moutlet4.outlet_type, Naturo.moutlet4.outlet_name, Naturo.moutlet4.latitude,Naturo.moutlet4.longitude, Naturo.moutlet4.created_at,Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name, Naturo.sales_mvisit.order_qty, Naturo.sales_mvisit.order_value FROM Naturo.sales_mvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_mvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_mvisit.time BETWEEN (SELECT CAST(DATEADD(MONTH,-6,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit) AND Naturo.moutlet4.created_at NOT BETWEEN (SELECT CAST(DATEADD(MONTH,-3,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit)) AS tb
            GROUP BY outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name),

            pc_tb AS
            (SELECT outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name, SUM(order_qty) as qty, SUM(order_value) as sale, COUNT(outlet_guid) as is_repeat_productive FROM
            (SELECT Naturo.sales_mvisit.visit_id, Naturo.sales_mvisit.is_productive, Naturo.sales_mvisit.outlet_guid, Naturo.moutlet4.outlet_type, Naturo.moutlet4.outlet_name, Naturo.moutlet4.latitude,Naturo.moutlet4.longitude, Naturo.moutlet4.created_at,Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name, Naturo.sales_mvisit.order_qty, Naturo.sales_mvisit.order_value FROM Naturo.sales_mvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_mvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_mvisit.time BETWEEN (SELECT CAST(DATEADD(MONTH,-6,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit) AND Naturo.sales_mvisit.is_productive=1 AND Naturo.moutlet4.created_at NOT BETWEEN (SELECT CAST(DATEADD(MONTH,-3,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit)) AS tb
            GROUP BY outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name)

            SELECT
                tc_tb.outlet_guid,
                tc_tb.outlet_type,
                tc_tb.outlet_name,
                tc_tb.latitude,
                tc_tb.longitude,
                tc_tb.created_at,
                tc_tb.region,
                tc_tb.territory,
                tc_tb.beat_name,
                tc_tb.qty,
                tc_tb.sale,
                tc_tb.is_repeat,
                COALESCE(pc_tb.is_repeat_productive, 0) AS is_repeat_productive
            FROM tc_tb
            LEFT JOIN pc_tb ON tc_tb.outlet_guid=pc_tb.outlet_guid
            WHERE tc_tb.is_repeat>=6 AND pc_tb.is_repeat_productive>=6 AND tc_tb.region IN {};
                    '''.format(str_states)
        df = pd.read_sql(sql=query1, con=conn1)
        df['productive_rate']=df['is_repeat_productive']/df['is_repeat']
        df.sort_values(by="productive_rate",ascending=True,inplace=True)
        
    else:
        with pyodbc.connect(conn_str) as conn1:
            query1 = '''
           WITH tc_tb AS
            (SELECT outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name, SUM(order_qty) as qty, SUM(order_value) as sale, COUNT(outlet_guid) as is_repeat FROM
            (SELECT Naturo.sales_mvisit.visit_id, Naturo.sales_mvisit.is_productive, Naturo.sales_mvisit.outlet_guid, Naturo.moutlet4.outlet_type, Naturo.moutlet4.outlet_name, Naturo.moutlet4.latitude,Naturo.moutlet4.longitude, Naturo.moutlet4.created_at,Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name, Naturo.sales_mvisit.order_qty, Naturo.sales_mvisit.order_value FROM Naturo.sales_mvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_mvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_mvisit.time BETWEEN (SELECT CAST(DATEADD(MONTH,-6,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit) AND Naturo.moutlet4.created_at NOT BETWEEN (SELECT CAST(DATEADD(MONTH,-3,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit)) AS tb
            GROUP BY outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name),

            pc_tb AS
            (SELECT outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name, SUM(order_qty) as qty, SUM(order_value) as sale, COUNT(outlet_guid) as is_repeat_productive FROM
            (SELECT Naturo.sales_mvisit.visit_id, Naturo.sales_mvisit.is_productive, Naturo.sales_mvisit.outlet_guid, Naturo.moutlet4.outlet_type, Naturo.moutlet4.outlet_name, Naturo.moutlet4.latitude,Naturo.moutlet4.longitude, Naturo.moutlet4.created_at,Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name, Naturo.sales_mvisit.order_qty, Naturo.sales_mvisit.order_value FROM Naturo.sales_mvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_mvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_mvisit.time BETWEEN (SELECT CAST(DATEADD(MONTH,-6,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit) AND Naturo.sales_mvisit.is_productive=1 AND Naturo.moutlet4.created_at NOT BETWEEN (SELECT CAST(DATEADD(MONTH,-3,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit)) AS tb
            GROUP BY outlet_guid, outlet_type, outlet_name, latitude, longitude, created_at, region, territory, beat_name)

            SELECT
                tc_tb.outlet_guid,
                tc_tb.outlet_type,
                tc_tb.outlet_name,
                tc_tb.latitude,
                tc_tb.longitude,
                tc_tb.created_at,
                tc_tb.region,
                tc_tb.territory,
                tc_tb.beat_name,
                tc_tb.qty,
                tc_tb.sale,
                tc_tb.is_repeat,
                COALESCE(pc_tb.is_repeat_productive, 0) AS is_repeat_productive
            FROM tc_tb
            LEFT JOIN pc_tb ON tc_tb.outlet_guid=pc_tb.outlet_guid
            WHERE tc_tb.is_repeat>=6 AND pc_tb.is_repeat_productive>=6;
                    '''
        df = pd.read_sql(sql=query1, con=conn1)
        df['productive_rate']=df['is_repeat_productive']/df['is_repeat']
        df.sort_values(by="productive_rate",ascending=True,inplace=True)

    return {"main_tb":df.reset_index(drop=True).to_dict(orient="records")}

@app.get("/kpi_prev_day/")
async def kpi_prev_day(states: str | None = None):
    if states:
        def remove_comma_before_parenthesis(text):
                # Regex pattern to find a comma before a closing parenthesis
                pattern = r',(?=\))'
                # Substitute the comma with an empty string
                result = re.sub(pattern, '', text)
                return result
        l_states=states.split(',')
        l_states=[i.strip() for i in l_states]
        str_states=str(tuple(l_states))
        str_states=remove_comma_before_parenthesis(str_states)

        with pyodbc.connect(conn_str) as conn1:
            query1 = '''
            SELECT Naturo.sales_mvisit.visit_id, Naturo.sales_mvisit.outlet_guid, Naturo.sales_mvisit.time, Naturo.sales_mvisit.order_qty, Naturo.sales_mvisit.net_value, Naturo.sales_mvisit.is_productive, Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name FROM Naturo.sales_mvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_mvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_mvisit.time BETWEEN (SELECT CAST(DATEADD(DAY,-1,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit) AND Naturo.moutlet4.region in {};
                    '''.format(str_states)
        df = pd.read_sql(sql=query1, con=conn1)
    else:
        with pyodbc.connect(conn_str) as conn1:
            query1 = '''
            SELECT Naturo.sales_mvisit.visit_id, Naturo.sales_mvisit.outlet_guid, Naturo.sales_mvisit.time, Naturo.sales_mvisit.order_qty, Naturo.sales_mvisit.net_value, Naturo.sales_mvisit.is_productive, Naturo.moutlet4.region, Naturo.moutlet4.territory, Naturo.moutlet4.beat_name FROM Naturo.sales_mvisit
            INNER JOIN Naturo.moutlet4 ON Naturo.sales_mvisit.outlet_guid=Naturo.moutlet4.outlet_guid
            WHERE Naturo.sales_mvisit.time BETWEEN (SELECT CAST(DATEADD(DAY,-1,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit);
                '''
        df = pd.read_sql(sql=query1, con=conn1)
    return {"main_tb":df.reset_index(drop=True).to_dict(orient="records")}

@app.get("/not_order_past_6ms/")
async def not_order_past_6ms(states: str | None = None):
    if states:
        def remove_comma_before_parenthesis(text):
                # Regex pattern to find a comma before a closing parenthesis
                pattern = r',(?=\))'
                # Substitute the comma with an empty string
                result = re.sub(pattern, '', text)
                return result
        l_states=states.split(',')
        l_states=[i.strip() for i in l_states]
        str_states=str(tuple(l_states))
        str_states=remove_comma_before_parenthesis(str_states)

        with pyodbc.connect(conn_str) as conn1: 
            query1 = '''
                    SELECT outlet_guid, SUM(order_qty) as order_qty, SUM(net_value) as net_value FROM
        (SELECT Naturo.sales_mvisit.visit_id, Naturo.sales_mvisit.outlet_guid, Naturo.sales_mvisit.time, Naturo.sales_mvisit.order_qty, Naturo.sales_mvisit.net_value FROM
        Naturo.sales_mvisit
        WHERE Naturo.sales_mvisit.time BETWEEN (SELECT CAST(DATEADD(MONTH,-6,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit)) AS tb
        GROUP BY outlet_guid;
            '''
        df = pd.read_sql(sql=query1, con=conn1)

        with pyodbc.connect(conn_str) as conn2: 
            query2 = '''
                    SELECT * 
        FROM Naturo.moutlet4
        WHERE Naturo.moutlet4.region In {};
            '''.format(str_states)
        
        df2=pd.read_sql(sql=query2, con=conn2)
    else:
        with pyodbc.connect(conn_str) as conn1:
            query1='''
                SELECT outlet_guid, SUM(order_qty) as order_qty, SUM(net_value) as net_value FROM
                (SELECT Naturo.sales_mvisit.visit_id, Naturo.sales_mvisit.outlet_guid, Naturo.sales_mvisit.time, Naturo.sales_mvisit.order_qty, Naturo.sales_mvisit.net_value FROM
                Naturo.sales_mvisit
                WHERE Naturo.sales_mvisit.time BETWEEN (SELECT CAST(DATEADD(MONTH,-6,(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit)) AS tb
                GROUP BY outlet_guid;
                '''
        df=pd.read_sql(sql=query1, con=conn1)

        with pyodbc.connect(conn_str) as conn2: 
            query2 = '''
                    SELECT * 
        FROM Naturo.moutlet4;
            '''
        df2=pd.read_sql(sql=query2, con=conn2)
    
    return {"main_tb":df2[(~df2['outlet_guid'].isin(list(df['outlet_guid'].unique())))].reset_index(drop=True).to_dict(orient="records"), "Total Number of Outlets":df2['outlet_guid'].nunique()}

@app.get("/not_order_past_nms/")
async def not_order_past_nms(n_months: int, states: str | None = None):
    if states:
        def remove_comma_before_parenthesis(text):
                # Regex pattern to find a comma before a closing parenthesis
                pattern = r',(?=\))'
                # Substitute the comma with an empty string
                result = re.sub(pattern, '', text)
                return result
        l_states=states.split(',')
        l_states=[i.strip() for i in l_states]
        str_states=str(tuple(l_states))
        str_states=remove_comma_before_parenthesis(str_states)

        with pyodbc.connect(conn_str) as conn1: 
            query1 = '''
                    SELECT outlet_guid, SUM(order_qty) as order_qty, SUM(net_value) as net_value FROM
        (SELECT Naturo.sales_mvisit.visit_id, Naturo.sales_mvisit.outlet_guid, Naturo.sales_mvisit.time, Naturo.sales_mvisit.order_qty, Naturo.sales_mvisit.net_value FROM
        Naturo.sales_mvisit
        WHERE Naturo.sales_mvisit.time BETWEEN (SELECT CAST(DATEADD(MONTH,-{},(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit)) AS tb
        GROUP BY outlet_guid;
            '''.format(n_months)
        df = pd.read_sql(sql=query1, con=conn1)

        with pyodbc.connect(conn_str) as conn2: 
            query2 = '''
                    SELECT * 
        FROM Naturo.moutlet4
        WHERE Naturo.moutlet4.region In {};
            '''.format(str_states)
        
        df2=pd.read_sql(sql=query2, con=conn2)
    else:
        with pyodbc.connect(conn_str) as conn1:
            query1='''
                SELECT outlet_guid, SUM(order_qty) as order_qty, SUM(net_value) as net_value FROM
                (SELECT Naturo.sales_mvisit.visit_id, Naturo.sales_mvisit.outlet_guid, Naturo.sales_mvisit.time, Naturo.sales_mvisit.order_qty, Naturo.sales_mvisit.net_value FROM
                Naturo.sales_mvisit
                WHERE Naturo.sales_mvisit.time BETWEEN (SELECT CAST(DATEADD(MONTH,-{},(SELECT MAX(time) FROM Naturo.sales_mvisit))as Date)) and (SELECT MAX(time) FROM Naturo.sales_mvisit)) AS tb
                GROUP BY outlet_guid;
                '''.format(n_months)
        df=pd.read_sql(sql=query1, con=conn1)

        with pyodbc.connect(conn_str) as conn2: 
            query2 = '''
                    SELECT * 
        FROM Naturo.moutlet4;
            '''
        df2=pd.read_sql(sql=query2, con=conn2)
    
    return {"main_tb":df2[(~df2['outlet_guid'].isin(list(df['outlet_guid'].unique())))].reset_index(drop=True).to_dict(orient="records"), "Total Number of Outlets":df2['outlet_guid'].nunique()}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
