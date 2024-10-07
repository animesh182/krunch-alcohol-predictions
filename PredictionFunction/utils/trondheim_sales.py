from datetime import date,timedelta
import pandas as pd
import decimal
import random
import numpy as np
from PredictionFunction.utils.constants import article_supergroup_values
import psycopg2
from PredictionFunction.utils.params import params
from PredictionFunction.utils.fetch_sales_data import fetch_salesdata
from io import BytesIO
import logging
from PredictionFunction.meta_tables import data

def sales_without_effect(company,start_date,end_date,alcohol_reference_restaurant,food_reference_restaurant):

    merged_sales = pd.read_csv('https://salespredictionstorage.blob.core.windows.net/csv/refrence_sales_alcohol.csv').reset_index()
    actual_trondheim_start_date=date(2024,2,1)

    trondheim_query = '''
        SELECT *
        FROM public."SalesData" 
        WHERE company = %s 
            AND restaurant = 'Trondheim' 
            AND date >= %s 
            AND date <= %s 
    '''

    with psycopg2.connect(**params) as conn:
        actual_trondheim_sales= pd.read_sql_query(trondheim_query,conn,params=[company,actual_trondheim_start_date,end_date])
    
    actual_trondheim_sales['gastronomic_day'] =pd.to_datetime(actual_trondheim_sales['gastronomic_day'])

    actual_trondheim_sales_grouped = actual_trondheim_sales.groupby(['gastronomic_day','article_supergroup'])['total_net'].sum().reset_index()
    final_merged = pd.concat([merged_sales,actual_trondheim_sales_grouped]).reset_index()
    # final_merged.drop_duplicates('gastronomic_day',keep='last',inplace=True)
    final_merged = final_merged[['gastronomic_day','article_supergroup','total_net']]
    final_merged.fillna(0,inplace=True)
    filtered_sales = final_merged.copy()
    # filtered_sales.to_csv('sales_trond.csv',index=False)
    return filtered_sales,actual_trondheim_start_date