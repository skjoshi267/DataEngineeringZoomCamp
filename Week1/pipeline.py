# # Import libraries
import datetime
from sqlite3 import connect
from urllib import response
import pandas as pd
from sqlalchemy import create_engine
import requests
import configparser
import gzip
from datetime import datetime

def update_table(data,cfg,connect,table_name):
    try:
        print(f'Uploading data to postgres {table_name}')
        data.to_sql(table_name,con=connect,if_exists="append",index=False)
        result = True
    except Exception as ex:
        print(f'Error with DB Insertion: {ex}')
        result = False
    finally:
        return result

def create_table(header_row,cfg):
    #"postgresql://admin:admin@postgres_sql:5432/nyc_taxi"
    try:
        connection_str = f'postgresql://{cfg["user"]}:{cfg["password"]}@{cfg["host"]}:{cfg["port"]}/{cfg["db"]}'
        engine = create_engine(connection_str)
        connect = engine.connect()
        table_name = cfg["url"][-30:].replace(".csv.gz","")
        print(f'Creating table with {table_name}')
        header_row.to_sql(table_name,con=connect,if_exists="replace",index=False)
        result = True
    except Exception as ex:
        print(f'Error with DB Operations: {ex}')
        result = False
    finally:
        return result,connect,table_name
    
def create_table_zone(header_row,cfg):
    #"postgresql://admin:admin@localhost:5432/nyc_taxi"
    try:
        connection_str = f'postgresql://{cfg["user"]}:{cfg["password"]}@{cfg["host"]}:{cfg["port"]}/{cfg["db"]}'
        engine = create_engine(connection_str)
        connect = engine.connect()
        table_name = f'nyc_taxi_zones'
        print(f'Creating table with {table_name}')
        header_row.to_sql(table_name,con=connect,if_exists="replace",index=False)
        result = True
    except Exception as ex:
        print(f'Error with DB Operations: {ex}')
        result = False
    finally:
        return result,connect,table_name

def process_data(file,cfg):
    print(f'Processing Data with {file}')
    with gzip.open(file,"rt") as file_name:
        data_itr = pd.read_csv(file_name,chunksize=100000,iterator=True)
        for idx,data in enumerate(data_itr):
            if idx == 0:
                response,connect,table_name = create_table(data.head(0),cfg)
                if not response: 
                    break
            
            data["lpep_pickup_datetime"] = pd.to_datetime(data["lpep_pickup_datetime"])
            data["lpep_dropoff_datetime"] = pd.to_datetime(data["lpep_dropoff_datetime"])
            print(f'Uploading records: {len(data)}')
            result = update_table(data,cfg,connect,table_name)
            if not result:
                break

def process_zone(file,cfg):
    print(f'Processing Data with {file}')
    data_itr = pd.read_csv(file,iterator=True)
    for idx,data in enumerate(data_itr):
        if idx == 0:
            response,connect,table_name = create_table_zone(data.head(0),cfg)
            if not response: 
                break
            
        print(f'Uploading records: {len(data)}')
        result = update_table(data,cfg,connect,table_name)
        if not result:
            break

def download_data(url):
    try:
        if "taxi_zone" in url:
            file_name = "zones.csv"
        else:
            file_name = "data.csv.gz"
        response = requests.get(url)
        if response.status_code == 200:
            with open(file_name,"wb") as file:
                file.write(response.content)
    except Exception as ex:
        print(f'Exception occured" {ex}')
        file_name = None
    finally:
        return file_name

def set_params():
    config = configparser.ConfigParser()
    config.read("config.ini")
    host = config["postgres-params"]["docker_container"]
    user = config["postgres-params"]["postgres_user"]
    password = config["postgres-params"]["postgres_pass"]
    database = config["postgres-params"]["postgres_db"]
    port = config["postgres-params"]["postgres_port"]
    url = config["postgres-params"]["download_url"]
    zones = config["postgres-params"]["zones"]
    
    print(f'Parameters read from config.ini')
    return {
        "host":host,
        "user":user,
        "password":password,
        "port":port,
        "db":database,
        "url":url,
        "zones":zones
        }
    
if __name__ == "__main__":
    cfg = set_params()
    
    if len(cfg["url"]):
        print(f'Downloading file from {cfg["url"]}')
        file  = download_data(cfg["url"])
        zones  = download_data(cfg["zones"])
        result = process_data(file,cfg)
        result_zones = process_zone(zones,cfg)
    else:
        print(f'Unable to download data. Invalid URL')



