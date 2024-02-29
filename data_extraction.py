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
from botocore import UNSIGNED 
from botocore.config import Config 

class DataExtractor:
    link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    headers = {"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}

    @staticmethod
    def read_rds_table(table, engine):
        query = pd.read_sql_table(table, engine)
        return query

    @staticmethod
    def retrieve_pdf_data(link):
        card_details = tabula.read_pdf(link, pages='all')
        card_details = pd.concat(card_details)
        return card_details

    @staticmethod
    def stores(url):
        headers = {"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        response = requests.get(url, headers=headers)
        return response.json()

    @staticmethod
    def list_number_of_stores():
        url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        return DataExtractor.stores(url)['number_stores']
    
    @staticmethod
    def retrieve_stores_data():
        url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
        stores_list = []
        for x in range(0, DataExtractor.list_number_of_stores()):
            burl = url+str(x)
            stores_list.append(DataExtractor.stores(burl))
        stores_list = pd.DataFrame(stores_list)
        return stores_list
    
    @staticmethod
    def extract_from_s3(file_path):
        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        s3.download_file('data-handling-public', 'products.csv', file_path)
        products = pd.read_csv(file_path, index_col=0)
        return products