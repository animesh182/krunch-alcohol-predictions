import pandas as pd
import numpy as np
import datetime

def predict(
    m, future, df, company, restaurant, start_date, end_date, prediction_category
):
    if future.empty:
        raise ValueError("The future DataFrame is empty.")

    ######### make forecast and plots

    # This must be changted to the correct opening hours, which varies for days and restaurants. Create dictionary for that and use here
    forecast = m.predict(future)
    # Set negative sales to 0 (this might happen sometimes for closed days)
    # Ensure negative sales predictions are set to 0
    forecast['yhat'] = np.where(forecast['yhat'] < 0, 0, forecast['yhat'])

    # Convert 'date' column in 'future' DataFrame to datetime if it's not already
    future['ds'] = pd.to_datetime(future['ds'])

    # future.to_csv("future_before_filtering.csv", index=False)


    # Filter out predictions for dates before today
    today = pd.to_datetime(datetime.datetime.now().date())
    future_predictions = future[future['ds'] > today]

    # future_predictions.to_csv("future_predictions.csv", index=False)


    # Join the forecast with the future predictions on the 'ds' column
    forecast_df = pd.DataFrame(forecast, columns=['yhat'])
    forecast_df = future_predictions.reset_index().join(forecast_df)  # Adjust as per your 'future' DataFrame's structure

    # Optionally, adjust the forecast based on other factors (like tourist data, if applicable)

    # Return the DataFrame with future dates and corresponding predictions
    return forecast_df[['ds', 'yhat']]

