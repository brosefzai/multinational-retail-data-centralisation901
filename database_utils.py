#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 11:04:32 2023

@author: frasier
"""

import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
        
    @staticmethod
    def read_db_creds():
        with open('db_creds.yaml', 'r') as file:
            dbcreds = yaml.safe_load(file)
        return dbcreds
    
    @staticmethod
    def init_db_engine(dbcreds):
        DATABASE_TYPE = dbcreds['DATABASE_TYPE']
        RDS_HOST =      dbcreds['RDS_HOST']
        RDS_PASSWORD =  dbcreds['RDS_PASSWORD']
        RDS_USER =      dbcreds['RDS_USER']
        RDS_DATABASE =  dbcreds['RDS_DATABASE']
        RDS_PORT =      dbcreds['RDS_PORT']
        engine =        create_engine(f"{DATABASE_TYPE}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
        return engine
        
    @staticmethod
    def list_db_tables(engine):
        inspector = inspect(engine)
        tables_list = inspector.get_table_names()
        return tables_list
    
    @staticmethod
    def upload_to_db(df, name_of_new_table, local_engine):
        df.to_sql(name_of_new_table, local_engine, if_exists='replace')
        
    @staticmethod
    def connect_to_local_db():
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = #'password'
        DATABASE = 'sales_data'
        PORT = 5432
        local_engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return local_engine
