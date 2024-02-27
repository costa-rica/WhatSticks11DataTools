import os
import json
from ..common.config_and_logger import config, logger_ws_analysis
from ..common.utilities import get_dateUserTz_3pm, \
    calculate_duration_in_hours
import pandas as pd
from ws_models import sess, inspect, engine, OuraSleepDescriptions, AppleHealthQuantityCategory
from datetime import datetime, timedelta
import pytz


# Function to run to get table of sleep time
def create_df_daily_sleep(df):
    logger_ws_analysis.info("- in create_df_daily_sleep")
    df_sleep = df[df['sampleType']=='HKCategoryTypeIdentifierSleepAnalysis'].copy()
    if len(df_sleep) == 0:
        return pd.DataFrame()#<-- return must return dataframe, expecting df on other end

    # Apply the function to each row to create the new dateUserTz_3pm column
    df_sleep['dateUserTz_3pm'] = df_sleep.apply(get_dateUserTz_3pm, axis=1)
    df_sleep_states_3_4_5 = df_sleep[df_sleep['value'].isin(["3.0", "4.0", "5.0"])]
    # Apply the function to each row in the dataframe
    df_sleep_states_3_4_5['sleepTimeUserTz'] = df_sleep_states_3_4_5.apply(lambda row: calculate_duration_in_hours(row['startDateUserTz'], row['endDateUserTz']), axis=1)
    # Now, let's aggregate by dateUserTz_3pm and sum the sleepTimeUserTz values
    aggregated_sleep_data = df_sleep_states_3_4_5.groupby('dateUserTz_3pm')['sleepTimeUserTz'].sum().reset_index()
    return aggregated_sleep_data

def create_df_n_minus1_daily_sleep(df_daily_sleep):
    logger_ws_analysis.info("- in create_df_n_minus1_daily_sleep")
    # # Convert back to 'YYYY-MM-DD' format if needed
    # df_daily_sleep['dateUserTz_3pm'] = df_daily_sleep['dateUserTz_3pm'].dt.strftime('%Y-%m-%d')
    df_daily_sleep['dateUserTz'] = pd.to_datetime(df_daily_sleep['dateUserTz'])
    # Subtract one day from each date in the column
    df_daily_sleep['dateUserTz'] = df_daily_sleep['dateUserTz'] - timedelta(days=1)
    # Convert back to 'YYYY-MM-DD' format if needed
    df_daily_sleep['dateUserTz'] = df_daily_sleep['dateUserTz'].dt.strftime('%Y-%m-%d')

    return df_daily_sleep