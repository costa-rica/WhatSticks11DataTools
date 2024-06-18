import os
import json
from ..common.config_and_logger import config, logger_ws_analysis
from ..common.utilities import get_startDate_3pm, \
    calculate_duration_in_hours
import pandas as pd
from datetime import datetime, timedelta
import pytz


### NOTE:
# Replacements:
# dateUserTz replaced by startDate_dateOnly
# dateUserTz_3pm replaced by startDate_dateOnly_sleep_adj
# sleepTimeUserTz replaced by sleep_duration
# startDateUserTz deleted
# endDateUserTz deleted



# Note: "df" parameter is strictly df from create_user_qty_cat_df
def create_df_daily_sleep(df):
    logger_ws_analysis.info("- in create_df_daily_sleep")
    logger_ws_analysis.info("- get code from Jupyter notebook -")

    df_sleep = df[df['sampleType']=='HKCategoryTypeIdentifierSleepAnalysis'].copy()
    df_sleep['startDate'] = pd.to_datetime(df_sleep['startDate'])
    df_sleep['endDate'] = pd.to_datetime(df_sleep['endDate'])
    df_sleep['startDate_dateOnly'] = pd.to_datetime(df_sleep['startDate_dateOnly'])
    if len(df_sleep) == 0:
        # return pd.DataFrame()#<-- return must return dataframe, expecting df on other end
        print("no data")
    else:
        
        # Apply the function to each row to create the new dateUserTz_3pm column
        # df_sleep['startDate'] = df_sleep.apply(get_dateUserTz_3pm, axis=1)
        df_sleep['startDate_dateOnly_sleep_adj'] = df_sleep.apply(get_startDate_3pm, axis=1)
        df_sleep_states_3_4_5 = df_sleep[df_sleep['value'].isin(["3.0", "4.0", "5.0", "3", "4", "5"])]

    df_sleep_states_3_4_5['sleep_duration'] = df_sleep_states_3_4_5.apply(lambda row: calculate_duration_in_hours(row['startDate'], row['endDate']), axis=1)
    aggregated_sleep_data = df_sleep_states_3_4_5.groupby('startDate_dateOnly_sleep_adj')['sleep_duration'].sum().reset_index()
    return aggregated_sleep_data


# # Note: "df" parameter is strictly df from create_user_qty_cat_df
# def create_df_daily_sleep_obe(df):
#     logger_ws_analysis.info("- in create_df_daily_sleep")
#     df_sleep = df[df['sampleType']=='HKCategoryTypeIdentifierSleepAnalysis'].copy()
#     if len(df_sleep) == 0:
#         return pd.DataFrame()#<-- return must return dataframe, expecting df on other end

#     # Apply the function to each row to create the new dateUserTz_3pm column
#     df_sleep['dateUserTz_3pm'] = df_sleep.apply(get_dateUserTz_3pm, axis=1)
#     df_sleep_states_3_4_5 = df_sleep[df_sleep['value'].isin(["3.0", "4.0", "5.0"])]

#     # Format of value column is different sometimes:
#     if len(df_sleep_states_3_4_5) == 0:
#         df_sleep_states_3_4_5 = df_sleep[df_sleep['value'].isin(["3", "4", "5"])]


#     # Apply the function to each row in the dataframe
#     df_sleep_states_3_4_5['sleepTimeUserTz'] = df_sleep_states_3_4_5.apply(lambda row: calculate_duration_in_hours(row['startDateUserTz'], row['endDateUserTz']), axis=1)
#     # Now, let's aggregate by dateUserTz_3pm and sum the sleepTimeUserTz values
#     aggregated_sleep_data = df_sleep_states_3_4_5.groupby('dateUserTz_3pm')['sleepTimeUserTz'].sum().reset_index()
#     return aggregated_sleep_data

def create_df_n_minus1_daily_sleep(df_daily_sleep):
    logger_ws_analysis.info("- in create_df_n_minus1_daily_sleep")
    # # Convert back to 'YYYY-MM-DD' format if needed
    # df_daily_sleep['dateUserTz_3pm'] = df_daily_sleep['dateUserTz_3pm'].dt.strftime('%Y-%m-%d')
    df_daily_sleep['startDate_dateOnly'] = pd.to_datetime(df_daily_sleep['startDate_dateOnly'])
    # Subtract one day from each date in the column
    df_daily_sleep['startDate_dateOnly'] = df_daily_sleep['startDate_dateOnly'] - timedelta(days=1)
    # Convert back to 'YYYY-MM-DD' format if needed
    df_daily_sleep['startDate_dateOnly'] = df_daily_sleep['startDate_dateOnly'].dt.strftime('%Y-%m-%d')

    return df_daily_sleep