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


#######################################
## Daily Workouts Dependent Variable ##
#######################################


def corr_workouts_sleep(df_workouts, df_qty_cat):

    logger_ws_analysis.info("- in corr_workouts_sleep")
    user_id = df_qty_cat['user_id'].iloc[0]
    df_daily_sleep = create_df_daily_sleep(df_qty_cat)# create daily sleep
    if len(df_daily_sleep) == 0:
        return "insufficient data", "insufficient data"
    df_daily_sleep.rename(columns=({'dateUserTz_3pm':'dateUserTz'}),inplace=True)

    logger_ws_analysis.info("-------- df_daily_sleep ------")
    logger_ws_analysis.info(df_daily_sleep.dtypes)
    logger_ws_analysis.info(len(df_daily_sleep))

    df_n_minus1_daily_sleep = create_df_n_minus1_daily_sleep(df_daily_sleep)
    df_n_minus1_daily_sleep['dateUserTz']=pd.to_datetime(df_n_minus1_daily_sleep['dateUserTz'])

    logger_ws_analysis.info("-------- df_n_minus1_daily_sleep ------")
    logger_ws_analysis.info(df_n_minus1_daily_sleep.dtypes)
    logger_ws_analysis.info(len(df_n_minus1_daily_sleep))
    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_n_minus1_daily_sleep.csv")
    df_n_minus1_daily_sleep.to_csv(csv_path_and_filename)


    df_daily_workout_duration = create_df_daily_workout_duration(df_workouts)
    df_daily_workout_duration['dateUserTz']=pd.to_datetime(df_daily_workout_duration['dateUserTz'])

    logger_ws_analysis.info("-------- df_daily_workout_duration ------")
    logger_ws_analysis.info(df_daily_workout_duration.dtypes)
    logger_ws_analysis.info(len(df_daily_workout_duration))
    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_duration.csv")
    df_daily_workout_duration.to_csv(csv_path_and_filename)

    try:
        if len(df_daily_workout_duration) > 5:# arbitrary minimum

            # This will keep only the rows that have matching 'dateUserTz' values in both dataframes
            df_daily_workout_duration_sleep_n_minus1 = pd.merge(df_n_minus1_daily_sleep,df_daily_workout_duration, on='dateUserTz')
            df_daily_workout_duration_sleep_n_minus1['dateUserTz'] = df_daily_workout_duration_sleep_n_minus1['dateUserTz'].dt.strftime('%Y-%m-%d')
            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_sleep_n_minus1.csv")
            df_daily_workout_duration_sleep_n_minus1.to_csv(csv_path_and_filename)
            # Calculate the correlation between step_count and sleepTimeUserTz
            correlation = df_daily_workout_duration_sleep_n_minus1['duration'].corr(df_daily_workout_duration_sleep_n_minus1['sleepTimeUserTz'])
            obs_count = len(df_daily_workout_duration_sleep_n_minus1)
            # logger_ws_analysis.info(f"correlation: {correlation}, corr type: {correlation}")
            logger_ws_analysis.info(f"df_daily_workout_duration_sleep_n_minus1 correlation: {correlation}, corr type: {type(correlation)}")
            return correlation, obs_count
        else:
            return "insufficient data", "insufficient data"
    except Exception as e:
        logger_ws_analysis.info(f"error in corr_workouts_sleep: {e}")
        return "insufficient data", "insufficient data"


def corr_workouts_steps(df_workouts, df_qty_cat):

    logger_ws_analysis.info("- in corr_workouts_steps")
    user_id = df_qty_cat['user_id'].iloc[0]
    # df_daily_sleep = create_df_daily_sleep(df_qty_cat)# create daily sleep
    df_daily_steps = create_df_daily_steps(df_qty_cat)# create daily steps
    if len(df_daily_steps) == 0:
        return "insufficient data", "insufficient data"
    # df_n_minus1_daily_sleep = create_df_n_minus1_daily_sleep(df_daily_sleep)
    df_n_minus1_daily_steps = create_df_n_minus1_daily_steps(df_daily_steps)
    df_n_minus1_daily_steps['dateUserTz']=pd.to_datetime(df_n_minus1_daily_steps['dateUserTz'])

    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_n_minus1_daily_steps.csv")
    df_n_minus1_daily_steps.to_csv(csv_path_and_filename)

    df_daily_workout_duration = create_df_daily_workout_duration(df_workouts)
    df_daily_workout_duration['dateUserTz']=pd.to_datetime(df_daily_workout_duration['dateUserTz'])

    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_duration.csv")
    df_daily_workout_duration.to_csv(csv_path_and_filename)

    try:
        if len(df_daily_workout_duration) > 5:# arbitrary minimum

            # This will keep only the rows that have matching 'dateUserTz' values in both dataframes
            # df_daily_workout_duration_sleep_n_minus1 = pd.merge(df_n_minus1_daily_sleep,df_daily_workout_duration, on='dateUserTz')
            df_daily_workout_duration_steps_n_minus1 = pd.merge(df_n_minus1_daily_steps,df_daily_workout_duration, on='dateUserTz')
            df_daily_workout_duration_steps_n_minus1['dateUserTz'] = df_daily_workout_duration_steps_n_minus1['dateUserTz'].dt.strftime('%Y-%m-%d')
            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_steps_n_minus1.csv")
            df_daily_workout_duration_steps_n_minus1.to_csv(csv_path_and_filename)
            logger_ws_analysis.info("--- df_daily_workout_duration_steps_n_minus1 ----")
            logger_ws_analysis.info(df_daily_workout_duration_steps_n_minus1.columns)
            logger_ws_analysis.info(df_daily_workout_duration_steps_n_minus1.head(2))
            # Calculate the correlation between step_count and sleepTimeUserTz
            # correlation = df_daily_workout_duration_sleep_n_minus1['duration'].corr(df_daily_workout_duration_sleep_n_minus1['sleepTimeUserTz'])
            correlation = df_daily_workout_duration_steps_n_minus1['duration'].corr(df_daily_workout_duration_steps_n_minus1['step_count'])
            obs_count = len(df_daily_workout_duration_steps_n_minus1)
            # logger_ws_analysis.info(f"correlation: {correlation}, corr type: {correlation}")
            logger_ws_analysis.info(f"df_daily_workout_duration_steps_n_minus1 correlation: {correlation}, corr type: {type(correlation)}")
            return correlation, obs_count
        else:
            return "insufficient data", "insufficient data"
    except Exception as e:
        logger_ws_analysis.info(f"error in corr_workouts_sleep: {e}")
        return "insufficient data", "insufficient data"


def corr_workouts_heart_rate(df_workouts, df_qty_cat):

    logger_ws_analysis.info("- in corr_workouts_heart_rate")
    user_id = df_qty_cat['user_id'].iloc[0]
    # df_daily_steps = create_df_daily_steps(df_qty_cat)# create daily steps
    df_daily_heart_rate = create_df_daily_heart_rate(df_qty_cat)# create daily steps
    if len(df_daily_heart_rate) == 0:
        return "insufficient data", "insufficient data"
    # df_n_minus1_daily_steps = create_df_n_minus1_daily_steps(df_daily_steps)
    df_n_minus1_daily_heart_rate = create_df_n_minus1_daily_heart_rate(df_daily_heart_rate)
    df_n_minus1_daily_heart_rate['dateUserTz']=pd.to_datetime(df_n_minus1_daily_heart_rate['dateUserTz'])

    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_n_minus1_daily_heart_rate.csv")
    df_n_minus1_daily_heart_rate.to_csv(csv_path_and_filename)

    df_daily_workout_duration = create_df_daily_workout_duration(df_workouts)
    df_daily_workout_duration['dateUserTz']=pd.to_datetime(df_daily_workout_duration['dateUserTz'])

    csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_duration.csv")
    df_daily_workout_duration.to_csv(csv_path_and_filename)

    try:
        if len(df_daily_workout_duration) > 5:# arbitrary minimum

            # This will keep only the rows that have matching 'dateUserTz' values in both dataframes
            # df_daily_workout_duration_steps_n_minus1 = pd.merge(df_n_minus1_daily_steps,df_daily_workout_duration, on='dateUserTz')
            df_daily_workout_duration_heart_rate_n_minus1 = pd.merge(df_n_minus1_daily_heart_rate,df_daily_workout_duration, on='dateUserTz')
            df_daily_workout_duration_heart_rate_n_minus1['dateUserTz'] = df_daily_workout_duration_heart_rate_n_minus1['dateUserTz'].dt.strftime('%Y-%m-%d')
            # save csv file for user
            csv_path_and_filename = os.path.join(config.DAILY_CSV, f"user_{user_id:04}_df_daily_workout_duration_heart_rate_n_minus1.csv")
            df_daily_workout_duration_heart_rate_n_minus1.to_csv(csv_path_and_filename)
            # logger_ws_analysis.info("--- df_daily_workout_duration_steps_n_minus1 ----")
            # logger_ws_analysis.info(df_daily_workout_duration_steps_n_minus1.columns)
            # logger_ws_analysis.info(df_daily_workout_duration_steps_n_minus1.head(2))
            # Calculate the correlation between step_count and sleepTimeUserTz
            # correlation = df_daily_workout_duration_sleep_n_minus1['duration'].corr(df_daily_workout_duration_sleep_n_minus1['sleepTimeUserTz'])
            correlation = df_daily_workout_duration_heart_rate_n_minus1['duration'].corr(df_daily_workout_duration_heart_rate_n_minus1['heart_rate_avg'])
            obs_count = len(df_daily_workout_duration_heart_rate_n_minus1)
            # logger_ws_analysis.info(f"correlation: {correlation}, corr type: {correlation}")
            logger_ws_analysis.info(f"df_daily_workout_duration_heart_rate_n_minus1 correlation: {correlation}, corr type: {type(correlation)}")
            return correlation, obs_count
        else:
            return "insufficient data", "insufficient data"
    except Exception as e:
        logger_ws_analysis.info(f"error in corr_workouts_sleep: {e}")
        return "insufficient data", "insufficient data"

