#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 02:48:19 2023

@author: frasier
"""

import database_utils
import data_extraction
import data_cleaning
import pandas as pd

if __name__ == '__main__':

    #=========================================================================
    #Instantiating classes and establishing the engines to connect our session 
    #to the remote and local databases
    #=========================================================================
    
    #Instantiating classes
    DataExtractor = data_extraction.DataExtractor()
    DatabaseConnector = database_utils.DatabaseConnector()
    DataCleaning = data_cleaning.DataCleaning()
    
    #Establishing connections between the local machine and the databases
    dbcreds = DatabaseConnector.read_db_creds()
    engine = DatabaseConnector.init_db_engine(dbcreds)
    
    #Remember to create a dictionary with the local database connection details 
    #called 'local creds' separately
    local_engine = DatabaseConnector.init_db_engine(local_creds)
    tables_list = DatabaseConnector.list_db_tables(engine)
    
    #==============================================
    #Extraction and cleaning of data for our tables
    #==============================================
    
    #Extract and clean user data
    users = DataExtractor.read_rds_table(tables_list[1], engine)
    users = DataCleaning.clean_user_data(users)
    DatabaseConnector.upload_to_db(users, 'dim_users', local_engine)

    #Extract and clean card data
    card_details = DataExtractor.retrieve_pdf_data(DataExtractor.link)
    card_details = DataCleaning.clean_card_data(card_details)
    DatabaseConnector.upload_to_db(card_details, 'dim_card_details', local_engine)
    
    #Extract and clean store data
    number_of_stores = DataExtractor.list_number_of_stores()
    stores_list = DataExtractor.retrieve_stores_data()
    stores_list = DataCleaning.clean_store_data(stores_list)
    DatabaseConnector.upload_to_db(stores_list, 'dim_store_details', local_engine)
    
    #Extract and clean products data - remember to change the local address in 
    #extract_from_s3() module to a location on your own machine
    products = DataExtractor.extract_from_s3()
    products = DataCleaning.convert_product_weights(products)
    products = DataCleaning.clean_products_data(products)
    DatabaseConnector.upload_to_db(products, 'dim_products', local_engine)

    #Extract and clean orders data
    orders = DataExtractor.read_rds_table(tables_list[2], engine)
    orders = DataCleaning.clean_orders_data(orders)
    DatabaseConnector.upload_to_db(orders, 'orders_table', local_engine)
    
    #Extract and clean dates data
    dates = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    dates = DataCleaning.clean_dates(dates)
    DatabaseConnector.upload_to_db(dates, 'dim_date_times', local_engine)
    
    
