from datetime import datetime, timezone
import logging
import azure.functions as func
import pandas as pd
import psycopg2
from PredictionFunction.meta_tables import (
    data,
    location_specific_dictionary,
)
from PredictionFunction.PredictionTypes.alcohol_mix_data_prep import (
    alcohol_sales_predictor as prepare_alcohol_data,
)
from PredictionFunction.predict import predict
from PredictionFunction.save_to_db import save_to_db
from datetime import timedelta
from PredictionFunction.utils.params import params


async def main(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    start_date = "2021-09-01"
    end_date = datetime.now().strftime("%Y-%m-%d")
    prediction_category = "alcohol"
    # prediction category is either hour, type, alcohol or day
    company = "Los Tacos"
    if myTimer.past_due:
        logging.info("The timer is past due!")

    for instance in data:
        restaurant = instance["Restaurant"]
        city = instance["City"]
        company = instance["Company"]
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cursor:
                end_date_query = '''
                SELECT MAX(gastronomic_day)
                FROM public."SalesData"
                WHERE restaurant = %s
            '''
                cursor.execute(end_date_query,(restaurant,))
                latest_gastronomic_day = cursor.fetchone()[0]
                logging.info(latest_gastronomic_day)
                if latest_gastronomic_day:
                    latest_date = latest_gastronomic_day - timedelta(days=1)
                    end_date= latest_date.strftime("%Y-%m-%d")
        conn.close()
        if restaurant == 'Oslo Torggata':
            start_date= "2022-05-12"
        if restaurant == 'Oslo Steen_Strom':
            start_date= "2022-02-01"
        if restaurant == 'Oslo Smestad':
            start_date= "2021-12-31"
        if restaurant == 'Alexander Kielland':
            start_date= "2024-04-10"
        if restaurant == 'Bjørvika':
            start_date= "2024-04-20"
        restaurant_func = location_specific_dictionary[restaurant]

        merged_data, historical_data, future_data = prepare_alcohol_data(
                company, restaurant, start_date, end_date
            )
       
        logging.info(f"Running predictions for {restaurant}")
        model, future_df, current_df = restaurant_func(
            prediction_category, restaurant, merged_data, historical_data, future_data
        )
        forecast = predict(
            model,
            future_df,
            current_df,
            company,
            restaurant,
            start_date,
            end_date,
            prediction_category,
        )
        save_to_db(forecast, company, restaurant, prediction_category)
