#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 11:05:46 2023

@author: frasier
"""

import numpy as np
import pandas as pd

#from dateutil.parser import parse

class DataCleaning():
    
    GB_regex = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
    US_regex = '^1?[-\. ]?(\(\d{3}\)?[-\. ]?|\d{3}?[-\. ]?)?\d{3}?[-\. ]?\d{4}$'
    DE_regex = '^((00|\+)49)?(0?[2-9][0-9]{1,})$'
#    def __init__(self):
#        self

    def clean_user_data(self, query):
#        mixed_date_df['dates'] = mixed_date_df['mixed_dates'].apply(parse)
        query = query.astype('string')
        query.date_of_birth = pd.to_datetime(query.date_of_birth, errors = 'coerce')
        query.join_date = pd.to_datetime(query.join_date, errors = 'coerce')
        query = query.replace('NULL', np.nan)
        query.dropna(axis=0, inplace=True)
        query['address'] = query['address'].str.replace('\n', ', ')
        query['address'] = query['address'].str.title()
        query.loc[query['country_code'] == 'US', 'address'] = query.loc[query['country_code'] == 'US', 'address'].str.slice_replace(start=-8, stop=-6, repl=query.loc[query['country_code'] == 'US', 'address'].str.slice(start=-8, stop=-6).str.upper())
        query['country_code'] = query['country_code'].str.replace('GGB', 'GB')
        query[query['country_code'] == 'GB'].loc[~query[query['country_code'] == 'GB']['phone_number'].str.match(GB_regex), 'phone_number'] = np.nan
        query[query['country_code'] == 'US'].loc[~query[query['country_code'] == 'US']['phone_number'].str.match(US_regex), 'phone_number'] = np.nan
        query[query['country_code'] == 'DE'].loc[~query[query['country_code'] == 'DE']['phone_number'].str.match(DE_regex), 'phone_number'] = np.nan
        query['phone_number'] = query['phone_number'].replace({r'\+44': '0', r'\+49': '0', r'\+1': '0', r'\(': '', r'\)': '', r'-': '', r' ': '', r'\.': '', r'x\d*$': ''}, regex=True)
        query.phone_number = query.phone_number.astype('int64')
        query.drop("index", axis=1, inplace=True)
        query.reset_index(drop=True, inplace=True)
        
        
    def clean_card_data(card_details):
        card_details = card_details.replace('NULL', np.nan)
        card_details.card_number = pd.to_numeric(card_details.card_number, errors='coerce')
        card_details.date_payment_confirmed = pd.to_datetime(card_details.date_payment_confirmed, errors='coerce')
        card_details.dropna(inplace=True)
        card_details.expiry_date = pd.to_datetime(card_details.expiry_date, format='%m/%y')
        card_details.card_provider = card_details.card_provider.astype('string')
        card_details.reset_index(drop=True, inplace=True)
    
    def clean_store_data(stores_list):
        stores_list.drop(columns=['index', 'lat'], axis = 1, inplace=True)
        stores_list.longitude = pd.to_numeric(stores_list.longitude, errors='coerce')
        stores_list.latitude = pd.to_numeric(stores_list.latitude, errors='coerce')
        stores_list = stores_list.replace('NULL', np.nan)
        stores_list.dropna(axis=0, inplace=True)
        stores_list['address'] = stores_list['address'].str.replace('\n', ', ')
        stores_list['continent'] = stores_list['continent'].str.replace('eeEurope', 'Europe')
        stores_list['continent'] = stores_list['continent'].str.replace('eeAmerica', 'America')
        stores_list['address'] = stores_list['address'].str.title()
        
    def convert_product_weights(products):
        products.weight.str[-2:].unique()
        products.dropna(axis=0, inplace=True)
        products['weight'] = products['weight'].str.replace('kg', '')
        products.weight[products.weight == '3 x 2g'] = (6/1000)
        products.weight[products['weight'].str.contains('x', case=True)] = (products.weight[products['weight'].str.contains('x', case=True)].str[:2].astype('int32') * products.weight[products['weight'].str.contains('x', case=True)].str[-4:].str.replace('g','').astype('int32'))/1000
        products.weight[products.weight == '77g .'] = '77g'
        products.weight[products.weight.astype(str).str.contains('g')] = (pd.to_numeric(products.weight[products.weight.astype(str).str.contains('g')].str.replace('g', '')) / 1000)
        products.drop(751, inplace=True)
        products.drop(1133, inplace=True)
        products.drop(1400, inplace=True)
        products.weight[products.weight.astype(str).str.contains('ml')] = pd.to_numeric(products.weight[products.weight.astype(str).str.contains('ml')].str.replace('ml',''))/1000
        products.weight[products.weight == '16oz'] = 0.4536
        products.weight = pd.to_numeric(products.weight)
        return products
        
    def clean_products_data(products):
        products.product_price = products.product_price.str.replace('£','')
        products.product_price = pd.to_numeric(products.product_price)
        products.loc[307, 'date_added'] = '2018-10-22'
        products.loc[1217, 'date_added'] = '2017-09-06'
        products.date_added = pd.to_datetime(products.date_added)
        
    def clean_orders_data(orders):
        orders.drop(columns=['first_name', 'last_name', '1'], inplace=True)
        orders.set_index('index', inplace=True, verify_integrity=True)
    
    def clean_dates(dates):
        dates_err = ['DXBU6GX1VC',
               'OEOXBP8X6D', 'NULL', '1Z18F4RM05', 'GT3JKF575H', 'CM5MTJKXMH',
               '5OQGE7K2AV', '1JCRGU3GIE', 'SQX52VSNMM', 'ALOGCWS9Y3',
               '7DNU2UWFP7', 'EOHYT5T70F', '5MUU1NKRED', '7RR8SRXQAW',
               'SSF9ANE440', '1PZDMCME1C', 'KQVJ34AINL', 'QA65EOIBX4',
               'YRYN6Y8SPJ', 'JMW951JPZC', 'DZC37NLW4F', 'SYID3PBQLP',
               'IXNB2XXEKB', 'MZIS9E7IXD']
        dates = dates[~dates['time_period'].isin(dates2_err)]
        dates['date_time'] = dates['year'] + '-' + dates['month'] + '-' + dates['day'] + ' ' + dates['timestamp']
        dates.date_time = pd.to_datetime(dates.date_time)
        dates.drop(columns=['timestamp', 'month', 'year', 'day'], inplace=True)
        dates.dropna(axis=0, inplace=True)
        
    
    crd2.to_sql(dim_card_details, local_engine, if_exists='replace')
    
       pd.to_datetime(card_details["expiry_date"], format='%m%y')
    
#          query.loc[query['country_code'] == 'US', 'address'] = query.loc[query['country_code'] == 'US', 'address'].str.slice_replace(start=-8, stop=-6, repl=query.loc[query['country_code'] == 'US', 'address'].str.slice(start=-8, stop=-6).str.upper())
#          
#          query.loc[query['country_code'] == 'GB', 'address'] = query.loc[query['country_code'] == 'GB', 'address'].str.slice_replace(start=-7, repl=users2.loc[users2['country_code'] == 'GB', 'address'].str.slice(start=-7).str.upper())
#          users2.loc[users2['country_code'] == 'GB', 'address'] = users2.loc[users2['country_code'] == 'GB', 'address'].str.slice_replace(start=-7, repl=users2.loc[users2['country_code'] == 'GB', 'address'].str.slice(start=-7).str.upper())
#          users2.loc[users2['country_code'] == 'GB', 'address'] = users2.loc[users2['country_code'] == 'GB', 'address'].str.slice_replace(start=-7, stop=-1, repl=users2.loc[users2['country_code'] == 'GB', 'address'].str.slice(start=-7).str.upper())
#          users2.loc[users2['country_code'] == 'GB', 'address'] = users2.loc[users2['country_code'] == 'GB', 'address'].str.slice_replace(start=-7, stop=-1, repl=users2.loc[users2['country_code'] == 'GB', 'address'].str.slice(start=-7).str.upper())

#          users2.loc[users2['country_code'] == 'GB', 'address'].str.slice_replace(start=-7, stop=-1, repl=users2.loc[users2['country_code'] == 'GB', 'address'].str.slice(start=-7).str.upper())
        
        
#          users2.loc[users2['country_code'] == 'GB', 'address'].str.slice_replace(start=-7, repl=users2.loc[users2['country_code'] == 'GB', 'address'].str.slice(start=-7).str.upper())


#products2.weight[products2['weight'].str.contains('g', case=False)]
#products2.weight[products2['weight'].str.contains('g', case=False)].str.replace('g', '')
#products2.weight[products2['weight'].str.contains('g', case=False)].str.replace('g', '').type(int)
#products2.weight[products2['weight'].str.contains('g', case=True)].str.replace('g', '').astype('float16').min()
#pd.to_numeric(products2.weight[products2['weight'].str.contains('g', case=True)].str.replace('g', ''))
#df.drop(#, inplace=True)
#products2.to_sql('dim_products', local_engine, if_exists='replace')        
        
        