import pandas as pd;import numpy as np
from ..common.config_and_logger import config, logger_ws_analysis
import os
from ws_analysis import create_user_qty_cat_df, create_df_daily_sleep, create_df_n_minus1_daily_sleep, \
    create_df_daily_steps


def corr_steps_sleep(df_qty_cat):
    logger_ws_analysis.info("- in corr_steps_sleep")

    df_qty_cat = create_user_qty_cat_df(1)
    user_id = df_qty_cat['user_id'].iloc[0]
    # Step 1: Create daily steps dataframe
    df_daily_steps = create_df_daily_steps(df_qty_cat)
    if len(df_daily_steps) == 0:
        logger_ws_analysis.info("- if len(df_daily_steps) == 0:")
        return "insufficient data", "insufficient data"
    df_daily_steps['dateUserTz']=pd.to_datetime(df_daily_steps['dateUserTz'])
    # Step 2: Create daily sleep n-1 df
    df_daily_sleep = create_df_daily_sleep(df_qty_cat)# create daily sleep
    if len(df_daily_sleep) == 0:
        logger_ws_analysis.info("- if len(df_daily_sleep) == 0:")
        return "insufficient data", "insufficient data"
    
    logger_ws_analysis.info("- in corr_steps_sleep ---> has enough df_daily_steps and df_daily_sleep")

    df_daily_sleep.rename(columns=({'dateUserTz_3pm':'dateUserTz'}),inplace=True)

    df_n_minus1_daily_sleep = create_df_n_minus1_daily_sleep(df_daily_sleep)
    df_n_minus1_daily_sleep['dateUserTz']=pd.to_datetime(df_n_minus1_daily_sleep['dateUserTz'])

    # This will keep only the rows that have matching 'dateUserTz' values in both dataframes
    df_daily_steps_sleep = pd.merge(df_daily_steps,df_n_minus1_daily_sleep, on='dateUserTz')
    df_daily_steps_sleep.dropna(inplace=True)

    if len(df_daily_steps_sleep) > 0:
        logger_ws_analysis.info("- if len(df_daily_steps_sleep) > 0:")

        try:
            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_steps_sleep.csv")
            df_daily_steps_sleep.to_csv(csv_path_and_filename)

            correlation = df_daily_steps_sleep['step_count'].corr(df_daily_steps_sleep['sleepTimeUserTz'])
            obs_count = len(df_daily_steps_sleep)
            # logger_ws_analysis.info(f"correlation: {correlation}, corr type: {correlation}")
            logger_ws_analysis.info(f"df_daily_steps_sleep correlation: {correlation}, obs_count: {obs_count}")
            return correlation, obs_count
        except Exception as e:
            logger_ws_analysis.info(f"error in corr_sleep_heart_rate: {e}")
            return "insufficient data", "insufficient data"
    else:
        logger_ws_analysis.info(f"- corr_steps_sleep had no observations")
        return "insufficient data", "insufficient data"