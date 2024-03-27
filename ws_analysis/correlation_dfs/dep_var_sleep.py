import pandas as pd;import numpy as np
from ..common.config_and_logger import config, logger_ws_analysis
import os
from ..daily_dfs.sleep_time import create_df_daily_sleep, \
    create_df_n_minus1_daily_sleep
from ..daily_dfs.steps import create_df_daily_steps, \
    create_df_n_minus1_daily_steps
from ..daily_dfs.heart_rate import create_df_daily_heart_rate, \
    create_df_n_minus1_daily_heart_rate
from ..daily_dfs.workouts import create_df_daily_workout_duration, \
    create_df_daily_workout_duration_dummies
from ..common.create_user_df import create_user_location_date_df
from ..daily_dfs.weather import create_df_weather_history

# df here would come from create_user_df create_user_qty_cat_df
def corr_sleep_steps(df):
    logger_ws_analysis.info("- in corr_sleep_steps")
    user_id = df['user_id'].iloc[0]

    df_daily_sleep = create_df_daily_sleep(df)
    if len(df_daily_sleep) == 0:
        return "insufficient data", "insufficient data"
    df_daily_sleep.rename(columns=({'dateUserTz_3pm':'dateUserTz'}),inplace=True)

    # if 'HKCategoryTypeIdentifierSleepAnalysis' in list_of_user_data:
    df_daily_steps = create_df_daily_steps(df)
    try:
        if len(df_daily_steps) > 5:# arbitrary minimum

            # This will keep only the rows that have matching 'dateUserTz' values in both dataframes
            df_daily_sleep_steps = pd.merge(df_daily_sleep,df_daily_steps, on='dateUserTz')
            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_sleep_steps.csv")
            df_daily_sleep_steps.to_csv(csv_path_and_filename)
            # Calculate the correlation between step_count and sleepTimeUserTz
            correlation = df_daily_sleep_steps['step_count'].corr(df_daily_sleep_steps['sleepTimeUserTz'])
            obs_count = len(df_daily_sleep_steps)
            # logger_ws_analysis.info(f"correlation: {correlation}, corr type: {correlation}")
            logger_ws_analysis.info(f"df_daily_sleep_steps correlation: {correlation}, corr type: {type(correlation)}")
            return correlation, obs_count
        else:
            return "insufficient data", "insufficient data"
    except Exception as e:
        logger_ws_analysis.info(f"error in corr_sleep_steps: {e}")
        return "insufficient data", "insufficient data"

def corr_sleep_heart_rate(df):
    logger_ws_analysis.info("- in corr_sleep_heart_rate")
    user_id = df['user_id'].iloc[0]

    df_daily_sleep = create_df_daily_sleep(df)
    if len(df_daily_sleep) == 0:
        return "insufficient data", "insufficient data"
    df_daily_sleep.rename(columns=({'dateUserTz_3pm':'dateUserTz'}),inplace=True)

    df_daily_heart_rate = create_df_daily_heart_rate(df)

    try:
        logger_ws_analysis.info("- try corr_sleep_heart_rate")
        if len(df_daily_heart_rate) > 5:# arbitrary minimum
            logger_ws_analysis.info("- if len(df_daily_heart_rate) > 5")

            # This will keep only the rows that have matching 'dateUserTz' values in both dataframes
            df_daily_sleep_heart_rate = pd.merge(df_daily_sleep,df_daily_heart_rate, on='dateUserTz')

            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_sleep_heart_rate.csv")
            df_daily_sleep_heart_rate.to_csv(csv_path_and_filename)

            # Calculate the correlation between step_count and sleepTimeUserTz
            correlation = df_daily_sleep_heart_rate['heart_rate_avg'].corr(df_daily_sleep_heart_rate['sleepTimeUserTz'])
            obs_count = len(df_daily_sleep_heart_rate)
            logger_ws_analysis.info(f"df_daily_sleep_heart_rate correlation: {correlation}, corr type: {type(correlation)}")
            return correlation, obs_count
        else:
            return "insufficient data", "insufficient data"
    except Exception as e:
        logger_ws_analysis.info(f"error in corr_sleep_heart_rate: {e}")
        return "insufficient data", "insufficient data"

def corr_sleep_workouts(df_qty_cat, df_workouts):

    logger_ws_analysis.info("- in corr_sleep_workouts")
    user_id = df_qty_cat['user_id'].iloc[0]
    df_daily_sleep = create_df_daily_sleep(df_qty_cat)
    if len(df_daily_sleep) == 0:
        return "insufficient data", "insufficient data"
    df_daily_sleep.rename(columns=({'dateUserTz_3pm':'dateUserTz'}),inplace=True)

    df_daily_workout_duration = create_df_daily_workout_duration(df_workouts)
    # df_daily_workout_duration_csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_duration.csv")
    # df_daily_workout_duration.to_csv(df_daily_workout_duration_csv_path_and_filename)
    try:
        if len(df_daily_workout_duration) > 5:# arbitrary minimum

            # This will keep only the rows that have matching 'dateUserTz' values in both dataframes
            df_daily_sleep_workout_duration = pd.merge(df_daily_sleep,df_daily_workout_duration, on='dateUserTz')
            # # save csv file for user
            # csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_sleep_workout_duration.csv")
            # df_daily_sleep_workout_duration.to_csv(csv_path_and_filename)
            # Calculate the correlation between step_count and sleepTimeUserTz
            correlation = df_daily_sleep_workout_duration['duration'].corr(df_daily_sleep_workout_duration['sleepTimeUserTz'])
            obs_count = len(df_daily_sleep_workout_duration)
            # logger_ws_analysis.info(f"correlation: {correlation}, corr type: {correlation}")
            logger_ws_analysis.info(f"df_daily_sleep_workout_duration correlation: {correlation}, corr type: {type(correlation)}")
            return correlation, obs_count
        else:
            return "insufficient data", "insufficient data"
    except Exception as e:
        logger_ws_analysis.info(f"error in corr_sleep_workouts: {e}")
        return "insufficient data", "insufficient data"

def corr_sleep_workout_dummies(df_qty_cat, df_workouts):

    logger_ws_analysis.info("- in corr_sleep_workout_dummies")
    user_id = df_qty_cat['user_id'].iloc[0]
    df_daily_sleep = create_df_daily_sleep(df_qty_cat)
    if len(df_daily_sleep) == 0:
        return "insufficient data", "insufficient data"
    df_daily_sleep.rename(columns=({'dateUserTz_3pm':'dateUserTz'}),inplace=True)

    df_daily_workout_duration_dummies = create_df_daily_workout_duration_dummies(df_workouts)
    try:
        if len(df_daily_workout_duration_dummies) > 5:# arbitrary minimum
            # This will keep only the rows that have matching 'dateUserTz' values in both dataframes
            df_daily_sleep_workout_duration = pd.merge(df_daily_sleep,df_daily_workout_duration_dummies, on='dateUserTz')
            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_sleep_workout_duration_dummies.csv")
            df_daily_sleep_workout_duration.to_csv(csv_path_and_filename)

            # List to store the tuples of column name and correlation
            col_names_and_correlations_tuple_list = []

            # Iterate over the columns to calculate correlation
            for col in df_daily_sleep_workout_duration.columns:
                if col.startswith('dur_') and col.endswith('_dummy'):
                    # Calculate the correlation
                    corr_value = df_daily_sleep_workout_duration['sleepTimeUserTz'].corr(df_daily_sleep_workout_duration[col])
                    
                    # Append the tuple (column name, correlation value) to the list
                    col_names_and_correlations_tuple_list.append((col, corr_value))

            obs_count = len(df_daily_sleep_workout_duration)

            return col_names_and_correlations_tuple_list, obs_count
        else:
            return "insufficient data", "insufficient data"
    except Exception as e:
        logger_ws_analysis.info(f"error in corr_sleep_workouts: {e}")
        return "insufficient data", "insufficient data"


# def corr_sleep_cloudiness(df_qty_cat, df_user_locations_day, df_weather_history):
def corr_sleep_cloudiness(df_qty_cat):
    ## Paremeters:
    ### df_qty_cat from create_user_qty_cat_df
    ### df_user_locations_day
    ### df_weather_history
    logger_ws_analysis.info("- in corr_sleep_cloudiness")
    user_id = df_qty_cat['user_id'].iloc[0]

    df_sleep_time = create_df_daily_sleep(df_qty_cat)
    df_sleep_time.rename(columns=({'dateUserTz_3pm':'dateUserTz'}),inplace=True)

    df_user_locations_day = create_user_location_date_df(user_id)
    df_weather_history = create_df_weather_history()


    # Step 2: Perform a left join to merge while keeping all rows from user_locations_day_df
    df_daily_cloudcover = pd.merge(df_user_locations_day, df_weather_history[['date_time', 'location_id', 'cloudcover']],
                        on=['date_time', 'location_id'], how='left')

    df_daily_cloudcover.rename(columns=({'date_time':'dateUserTz'}),inplace=True)


    df_daily_sleep_time_cloudcover = pd.merge(df_sleep_time, df_daily_cloudcover[['dateUserTz','cloudcover']],
                                          on=['dateUserTz'],how='left')
                                          
    correlation = df_daily_sleep_time_cloudcover['cloudcover'].corr(df_daily_sleep_time_cloudcover['sleepTimeUserTz'])

    # TODO: Remove n/a's
