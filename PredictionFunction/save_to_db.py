import datetime
import uuid
from datetime import timezone
import logging

import pandas as pd
from PredictionFunction.PredictionSaver.saveAlcoholPredictions import save_alcohol_predictions

def save_to_db(forecast_df, company, restaurant, prediction_category):
    unwanted_columns = [
        "_lower",
        "_upper",
        "Unnamed",
        "custom_regressor",
        "extra_regressors_additive",
        "trend",
        "additive_terms",
        "yearly",
        "index",
        "0",
        "holidays",
        "weekly_1",
        "weekly_2",
        "weekly_3",
        "weekly_4",
        "weekly_5",
        "weekly_6",
    ]
    filtered_columns = [
        col
        for col in forecast_df.columns
        if not any(unwanted in col for unwanted in unwanted_columns)
    ]
    filtered_df = forecast_df[filtered_columns]


    prediction_data = filtered_df[["ds", "yhat"]]
    if prediction_category == "alcohol":
        prediction_data = prediction_data.rename(
                columns={"ds": "date", "yhat": "product_mix"}
            )
        prediction_data["date"] = pd.to_datetime(prediction_data["date"]).dt.date
        # logging.info(prediction_data["date"][0])
        prediction_data["company"] = company
        prediction_data["restaurant"] = restaurant
        prediction_data["product_mix"] = prediction_data["product_mix"].astype(float)
        prediction_data["id"] = [uuid.uuid4() for _ in range(len(prediction_data))]
        prediction_data["created_at"] = datetime.datetime.now()
        # logging.info(prediction_data["created_at"][0])
        prediction_data["created_at"] = prediction_data["created_at"].apply(
                lambda x: x.strftime("%Y-%m-%d %H:%M:%S")
            )
        prediction_data["id"] = prediction_data["id"].apply(str)
        save_alcohol_predictions(prediction_data, restaurant)



    print(f"Finished prediction for {restaurant} of {company}")
