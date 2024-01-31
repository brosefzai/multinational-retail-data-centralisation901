#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 02:48:19 2023

@author: frasier
"""

import database_utils
import data_extraction
#import data_extraction
#import data_cleaning

if __name__ == '__main__':
    connector = database_utils.DatabaseConnector()
    engine = connector.init_db_engine()
    tables_list = connector.list_db_tables(engine)
#    print(tables_list)
    query = data_extraction.DataExtractor().read_rds_table(tables_list[1], engine)
    print(query)
    
#Note: if pgadmin4 isn't opening, find the log file of the session, and open 
#the link contained within right at the end. It'll open up in the browser