#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 11:00:04 2023

@author: frasier
"""

import pandas as pd
import tabula
import requests
import boto3
import botocore 
from botocore import UNSIGNED 
from botocore.config import Config 
#from sqlachemy import text
#import database_utils

from database_utils import DatabaseConnector

class DataExtractor:
    
    headers = {"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
            
    def read_rds_table(self, table, engine):
        query = pd.read_sql_table(table, engine)
        return query
    
    def retrieve_pdf_data(link):
        card_details = tabula.read_pdf(link, pages='all')
        card_details = pd.concat(card_details)
        return card_details
    
    def stores(url):
        headers = {"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        response = requests.get(url, headers=headers)
        return response.json()

    def list_number_of_stores():
        url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        return stores(url)['number_stores']
    
    def retrieve_stores_data():
        url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
        stores_list = []
        for x in range(0, list_number_of_stores()):
            burl = url+str(x)
            stores_list.append(stores(burl))
        stores_list = pd.DataFrame(stores_list)
        return stores_list
    
    def extract_from_s3():
        s3 = boto3.client('s3')
        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        s3.download_file('data-handling-public', 'products.csv', '/Users/frasier/Desktop/Python/python_work/multinational-retail-data-centralisation901/products.csv')
        products = pd.read_csv("products.csv", index_col=0)


if __name__ == '__main__':
    connector = DatabaseConnector()
    engine = connector.init_db_engine()
    tables_list = connector.list_db_tables(engine)
    query = DataExtractor().read_rds_table(tables_list[1], engine)
    print(query)
    
    
    

#import boto3 
#import botocore 
#from botocore import UNSIGNED 
#from botocore.config import Config 

#BUCKET_NAME = 'data-handling-public' PATH = 's3://data-handling-public/products.csv' 
#s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))





#    extractor = DataExtractor(connector)
#    extractor.read_rds_table()
#    .

#    def __init__(self, connector):
#        self.connector = connector
#    def __init__(self, engine, query):#, tables_list):
#        self.engine = engine
#        self.query = query
#        self.tables_list = tables_list
        
#    def retrieve(sql):
#        with engine.connect() as connection:
#            result = connection.execute(text(sql))
#            for row in result:
#                print(row)

#    def read_rds_table1(query):
#        with engine.connect() as connection:
#            df = pd.read_sql_query(query, connection)