#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 11:05:46 2023

@author: frasier
"""

import numpy as np
import pandas as pd

class DataCleaning:
    
    GB_regex = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
    US_regex = '^1?[-\. ]?(\(\d{3}\)?[-\. ]?|\d{3}?[-\. ]?)?\d{3}?[-\. ]?\d{4}$'
    DE_regex = '^((00|\+)49)?(0?[2-9][0-9]{1,})$'
    

    @staticmethod
    def clean_user_data(query):
        query.join_date = pd.to_datetime(query.join_date, format='mixed', errors = 'coerce')
        query.date_of_birth = pd.to_datetime(query.date_of_birth, format='mixed', errors = 'coerce')
        query.dropna(inplace=True)
        query['address'] = query['address'].str.replace('\n', ', ')
        query['address'] = query['address'].str.title()
        query['country_code'] = query['country_code'].str.replace('GGB', 'GB')
        query[query['country_code'] == 'GB'].loc[~query[query['country_code'] == 'GB']['phone_number'].str.match(GB_regex), 'phone_number'] = np.nan
        query[query['country_code'] == 'US'].loc[~query[query['country_code'] == 'US']['phone_number'].str.match(US_regex), 'phone_number'] = np.nan
        query[query['country_code'] == 'DE'].loc[~query[query['country_code'] == 'DE']['phone_number'].str.match(DE_regex), 'phone_number'] = np.nan
        query['phone_number'] = query['phone_number'].replace({r'\(0\)': ''}, regex=True)
        query['phone_number'] = query['phone_number'].replace({r'\+44': '0', r'\+49': '0', r'\+1': '0', r'\(': '', r'\)': '', r'-': '', r' ': '', r'\.': '', r'x\d*$': ''}, regex=True)
        query.phone_number = query.phone_number.astype('int64')
        query.drop("index", axis=1, inplace=True)
        query.reset_index(drop=True, inplace=True)
        
    @staticmethod
    def clean_card_data(card_details):
        card_details = card_details.replace('NULL', np.nan)
        card_details.dropna(inplace=True)
        cards_err = ['NB71VBAHJE','WJVMUO4QX6', 'JRPRLPIBZ2', 'TS8A81WFXV', 'JCQMU8FN85', '5CJH7ABGDR', 'DE488ORDXY', 'OGJTXI6X1H', '1M38DYQTZV', 'DLWF2HANZF', 'XGZBYBYGUW', 'UA07L7EILH', 'BU9U947ZGV', '5MFWFBZRM9']
        card_details = card_details[~card_details['card_provider'].isin(cards_err)]
        card_details['card_number'][card_details['card_number'].str.contains('[^0-9]', na=False)] = card_details['card_number'][card_details['card_number'].str.contains('[^0-9]', na=False)].str.replace('?','')
    
    @staticmethod
    def clean_store_data(stores):
        stores.dropna(axis=0, inplace=True)
        stores['address'] = stores['address'].str.replace('\n', ', ')
        stores['continent'] = stores['continent'].str.replace('eeEurope', 'Europe')
        stores['continent'] = stores['continent'].str.replace('eeAmerica', 'America')
        stores['address'] = stores['address'].str.title()
        stores_err = ['YELVM536YT', 'FP8DLXQVGH', 'HMHIFNLOBN', 'F3AO8V2LHU', 'OH20I92LX3', 'OYVW925ZL8', 'B3EH2ZGQAV']
        stores = stores[~stores['country_code'].isin(stores_err)]
        stores.drop(217, inplace=True)
        stores.drop(405, inplace=True)
        stores.drop(437, inplace=True)
        stores_staff_alphanum_rows = [31, 179, 248, 341, 375]
        stores.loc[stores_staff_alphanum_rows, 'staff_numbers'].str.replace('[^0-9]', '', regex=True)
        stores.at[0, 'latitude'] = 0
        stores.at[0, 'longitude'] = 0
            
    @staticmethod
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
        
    @staticmethod
    def clean_products_data(products):
        products.product_price = products.product_price.str.replace('£','')
        products.product_price = pd.to_numeric(products.product_price)
        products.loc[307, 'date_added'] = '2018-10-22'
        products.loc[1217, 'date_added'] = '2017-09-06'
        products.date_added = pd.to_datetime(products.date_added)
        
    @staticmethod
    def clean_orders_data(orders):
        orders.drop(columns=['first_name', 'last_name', '1'], inplace=True)
        orders.set_index('index', inplace=True, verify_integrity=True)
    
    @staticmethod
    def clean_dates(dates):
        dates_err = ['DXBU6GX1VC',
               'OEOXBP8X6D', 'NULL', '1Z18F4RM05', 'GT3JKF575H', 'CM5MTJKXMH',
               '5OQGE7K2AV', '1JCRGU3GIE', 'SQX52VSNMM', 'ALOGCWS9Y3',
               '7DNU2UWFP7', 'EOHYT5T70F', '5MUU1NKRED', '7RR8SRXQAW',
               'SSF9ANE440', '1PZDMCME1C', 'KQVJ34AINL', 'QA65EOIBX4',
               'YRYN6Y8SPJ', 'JMW951JPZC', 'DZC37NLW4F', 'SYID3PBQLP',
               'IXNB2XXEKB', 'MZIS9E7IXD']
        dates = dates[~dates['time_period'].isin(dates_err)]
        dates['date_time'] = dates['year'] + '-' + dates['month'] + '-' + dates['day'] + ' ' + dates['timestamp']
        dates.date_time = pd.to_datetime(dates.date_time)
        dates.drop(columns=['timestamp', 'month', 'year', 'day'], inplace=True)
        dates.dropna(axis=0, inplace=True)
