#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 02:48:19 2023

@author: frasier
"""

import database_utils
#import data_extraction
#import data_cleaning

connector = database_utils.DatabaseConnector()
engine = connector.init_db_engine()
tables_list = connector.list_db_tables(engine)
print(tables_list)