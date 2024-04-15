import pandas as pd
from ws_analysis import create_user_qty_cat_df, corr_sleep_steps, corr_sleep_heart_rate, \
    create_user_workouts_df, corr_sleep_workouts, corr_workouts_sleep, \
    create_df_daily_workout_duration, corr_workouts_steps, corr_sleep_workout_dummies, \
    corr_workouts_heart_rate, \
    corr_sleep_cloudiness, corr_sleep_temperature, corr_workouts_cloudiness, \
    corr_workouts_temperature
# from common.config_and_logger import config, logger_ws_utilities
from ..common.config_and_logger import config, logger_ws_utilities
# from ..common.utilities import wrap_up_session
import os
import numpy as np

def user_sleep_time_correlations(user_id):
    logger_ws_utilities.info(f"- in user_sleep_time_correlations for user_id: {user_id} ")

    # df_qty_cat, sampleTypeListQtyCat = create_user_qty_cat_df(user_id=user_id,user_tz_str=timezone_str)
    # df_qty_cat, sampleTypeListQtyCat = create_user_qty_cat_df(user_id=user_id)
    df_qty_cat = create_user_qty_cat_df(user_id=user_id)
    # if sampleTypeListQtyCat != "insufficient data":
    if len(df_qty_cat) > 0:
        list_of_arryIndepVarObjects_dict = []
        # if 'HKCategoryTypeIdentifierSleepAnalysis' in sampleTypeListQtyCat:
        logger_ws_utilities.info(f"- There are Sleep Analysis rows")
        arryIndepVarObjects_dict = {}
        # Steps
        # if 'HKQuantityTypeIdentifierStepCount' in sampleTypeListQtyCat:
        correlation_value, obs_count = corr_sleep_steps(df_qty_cat)
        if isinstance(obs_count, int):

            arryIndepVarObjects_dict["independentVarName"]= "Step Count"
            arryIndepVarObjects_dict["forDepVarName"]= "Sleep Time"
            # correlation_value, obs_count = corr_sleep_steps(df_qty_cat)
            arryIndepVarObjects_dict["correlationValue"]= correlation_value
            arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
            arryIndepVarObjects_dict["definition"]= "The count of your daily steps"
            arryIndepVarObjects_dict["noun"]= "daily step count"
            list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)
            flag_data_for_dashboard_exisits=True

        # Heart Rate
        # if 'HKQuantityTypeIdentifierHeartRate' in sampleTypeListQtyCat:
        correlation_value, obs_count = corr_sleep_heart_rate(df_qty_cat)
        if isinstance(obs_count, int):

            arryIndepVarObjects_dict = {}
            arryIndepVarObjects_dict["independentVarName"]= "Heart Rate"
            arryIndepVarObjects_dict["forDepVarName"]= "Sleep Time"
            # correlation_value, obs_count = corr_sleep_heart_rate(df_qty_cat)
            arryIndepVarObjects_dict["correlationValue"]= correlation_value
            arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
            arryIndepVarObjects_dict["definition"]= "The avearge of heart rates recorded across all your devices"
            arryIndepVarObjects_dict["noun"]= "daily average heart rate"
            list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)


        else:
            logger_ws_utilities.info(f"- There are NO Sleep Analysis rows !!!! -")
        # check if user has workouts
        # df_workouts, sampleTypeListWorkouts = create_user_workouts_df(user_id,user_tz_str=timezone_str)
        # df_workouts, sampleTypeListWorkouts = create_user_workouts_df(user_id)
        df_workouts = create_user_workouts_df(user_id)
        # if sampleTypeListWorkouts != "insufficient data":
        if len(df_workouts) > 0:
            # Workouts 
            correlation_value, obs_count = corr_sleep_workouts(df_qty_cat, df_workouts)
            if isinstance(obs_count, int):
                arryIndepVarObjects_dict = {}
                arryIndepVarObjects_dict["independentVarName"]= "Workout Duration"
                arryIndepVarObjects_dict["forDepVarName"]= "Sleep Time"
                # correlation_value, obs_count = corr_sleep_workouts(df_qty_cat, df_workouts)
                arryIndepVarObjects_dict["correlationValue"]= correlation_value
                arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
                arryIndepVarObjects_dict["definition"]= "The avearge of daily duration recorded by all your devices and apps that share with Apple Health"
                arryIndepVarObjects_dict["noun"]= "avearge daily minutes worked out"
                list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)
            # #####################################################################################################################################
            # # NOTE: Skipping Workout dummies for now 2024-04-09 #
            # # Workout duration dummies
            # if len(df_workouts) > 5:
            #     col_names_and_correlations_tuple_list, obs_count = corr_sleep_workout_dummies(df_qty_cat, df_workouts)

            #     if col_names_and_correlations_tuple_list != "insufficient data":
            #         # Filter out tuples with NaN values
            #         filtered_list = [t for t in col_names_and_correlations_tuple_list if not np.isnan(t[1])]

            #         # Find the tuple with the largest absolute correlation value
            #         largest_correlation_tuple = max(filtered_list, key=lambda x: abs(x[1]), default=None)

            #         upper_value_string = largest_correlation_tuple[0][len("dur_"):-len("_dummy")]
            #         lower_value_string = int(upper_value_string)-10
            #         indep_var_name = f"Workouts of {lower_value_string} to {upper_value_string} minutes"

            #         arryIndepVarObjects_dict = {}
            #         arryIndepVarObjects_dict["independentVarName"]= indep_var_name
            #         arryIndepVarObjects_dict["forDepVarName"]= "Sleep Time"
            #         # correlation_value, obs_count = corr_sleep_workouts(df_qty_cat, df_workouts)
            #         correlation_value, obs_count = largest_correlation_tuple[1], obs_count
            #         arryIndepVarObjects_dict["correlationValue"]= correlation_value
            #         arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
                    
            #         arryIndepVarObjects_dict["definition"]= f"Workouts that last between {lower_value_string} and {upper_value_string} minutes"
            #         arryIndepVarObjects_dict["noun"]= f"workouts between {int(upper_value_string)-10} and {upper_value_string} minutes"
            #         list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)
            # #####################################################################################################################################

        else:
            logger_ws_utilities.info(f"- There are NO Workout rows !!!! -")
        

        # Cloudiness
        correlation_value, obs_count = corr_sleep_cloudiness(df_qty_cat)
        logger_ws_utilities.info(f"- correlation_value, obs_count: {correlation_value}, {obs_count} -")
        if isinstance(obs_count, int):
            arryIndepVarObjects_dict = {}
            arryIndepVarObjects_dict["independentVarName"]= "Cloud Cover"
            arryIndepVarObjects_dict["forDepVarName"]= "Sleep Time"
            arryIndepVarObjects_dict["correlationValue"]= correlation_value
            arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
            arryIndepVarObjects_dict["definition"]= "An index capturing cloudiness in a day of your location."
            arryIndepVarObjects_dict["noun"]= "daily cloud cover"
            list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)
        else:
            logger_ws_utilities.info(f"- No weather -")

        # Temperature
        correlation_value, obs_count = corr_sleep_temperature(df_qty_cat)
        logger_ws_utilities.info(f"- correlation_value, obs_count: {correlation_value}, {obs_count} -")
        if isinstance(obs_count, int):
            arryIndepVarObjects_dict = {}
            arryIndepVarObjects_dict["independentVarName"]= "Temperature"
            arryIndepVarObjects_dict["forDepVarName"]= "Sleep Time"
            arryIndepVarObjects_dict["correlationValue"]= correlation_value
            arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
            arryIndepVarObjects_dict["definition"]= "The average temperature in a day of your location."
            arryIndepVarObjects_dict["noun"]= "daily average temperature"
            list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)
        else:
            logger_ws_utilities.info(f"- No weather -")
    try:
        for indVarObj in list_of_arryIndepVarObjects_dict:
            if indVarObj.get('correlationValue') != 'insufficient data':
                logger_ws_utilities.info(f"- user_sleep_time_correlations (json dict sent to iOS -")
                logger_ws_utilities.info(f"{list_of_arryIndepVarObjects_dict}")

                return list_of_arryIndepVarObjects_dict
    except:
        return []


# def user_workouts_duration_correlations(user_id,user_tz_str):
def user_workouts_duration_correlations(user_id):
    logger_ws_utilities.info("- in user_workouts_duration_correlations ")

    # df_qty_cat, sampleTypeListQtyCat = create_user_qty_cat_df(user_id=user_id)
    df_qty_cat = create_user_qty_cat_df(user_id=user_id)

    # df_workouts, sampleTypeListWorkouts = create_user_workouts_df(user_id)
    df_workouts = create_user_workouts_df(user_id)
    # if "insufficient data" not in [sampleTypeListQtyCat, sampleTypeListWorkouts]:
    if len(df_qty_cat) > 0 and len(df_workouts) > 0:
        df_daily_workouts = create_df_daily_workout_duration(df_workouts)
        if len(df_workouts) > 5:
            list_of_arryIndepVarObjects_dict = []
            # if 'HKCategoryTypeIdentifierSleepAnalysis' in sampleTypeListQtyCat:
            correlation_value, obs_count = corr_workouts_sleep(df_workouts, df_qty_cat)
            if isinstance(obs_count, int):
                arryIndepVarObjects_dict = {}
                arryIndepVarObjects_dict["independentVarName"]= "Sleep Time"
                arryIndepVarObjects_dict["forDepVarName"]= "Workout Duration"
                # correlation_value, obs_count = corr_workouts_sleep(df_workouts, df_qty_cat)
                arryIndepVarObjects_dict["correlationValue"]= correlation_value
                arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
                arryIndepVarObjects_dict["definition"]= "The number of hours slept in the previous night"
                arryIndepVarObjects_dict["noun"]= "hours of sleep the night before"
                list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)

                
            # Steps
            correlation_value, obs_count = corr_workouts_steps(df_workouts, df_qty_cat)
            # if 'HKQuantityTypeIdentifierStepCount' in sampleTypeListQtyCat:
            if isinstance(obs_count, int):
                arryIndepVarObjects_dict = {}
                arryIndepVarObjects_dict["independentVarName"]= "Step Count"
                arryIndepVarObjects_dict["forDepVarName"]= "Workout Duration"
                # correlation_value, obs_count = corr_sleep_steps(df_qty_cat)
                # correlation_value, obs_count = corr_workouts_steps(df_workouts, df_qty_cat)
                arryIndepVarObjects_dict["correlationValue"]= correlation_value
                arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
                arryIndepVarObjects_dict["definition"]= "The count of your daily steps the previous day"
                arryIndepVarObjects_dict["noun"]= "daily step count"
                list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)

            # Heart Rate
            correlation_value, obs_count = corr_workouts_heart_rate(df_workouts, df_qty_cat)
            # if 'HKQuantityTypeIdentifierHeartRate' in sampleTypeListQtyCat:
            if isinstance(obs_count, int):
                arryIndepVarObjects_dict = {}
                arryIndepVarObjects_dict["independentVarName"]= "Heart Rate"
                arryIndepVarObjects_dict["forDepVarName"]= "Workout Duration"
                # correlation_value, obs_count = corr_sleep_heart_rate(df_qty_cat)
                # correlation_value, obs_count = corr_sleep_heart_rate(df_qty_cat)# <--- this changes
                arryIndepVarObjects_dict["correlationValue"]= correlation_value
                arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
                arryIndepVarObjects_dict["definition"]= "The avearge of heart rates, from the prior day, recorded across all your devices"
                arryIndepVarObjects_dict["noun"]= "daily average heart rate"
                list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)


            # Cloudiness
            correlation_value, obs_count = corr_workouts_cloudiness(df_workouts)
            logger_ws_utilities.info(f"- correlation_value, obs_count: {correlation_value}, {obs_count} -")
            if isinstance(obs_count, int):
                arryIndepVarObjects_dict = {}
                arryIndepVarObjects_dict["independentVarName"]= "Cloud Cover"
                arryIndepVarObjects_dict["forDepVarName"]= "Workout Duration"
                arryIndepVarObjects_dict["correlationValue"]= correlation_value
                arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
                arryIndepVarObjects_dict["definition"]= "An index capturing cloudiness in a day of your location."
                arryIndepVarObjects_dict["noun"]= "daily cloud cover"
                list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)
            else:
                logger_ws_utilities.info(f"- No weather -")

            # Temperature
            correlation_value, obs_count = corr_workouts_temperature(df_workouts)
            logger_ws_utilities.info(f"- correlation_value, obs_count: {correlation_value}, {obs_count} -")
            if isinstance(obs_count, int):
                arryIndepVarObjects_dict = {}
                arryIndepVarObjects_dict["independentVarName"]= "Temperature"
                arryIndepVarObjects_dict["forDepVarName"]= "Workout Duration"
                arryIndepVarObjects_dict["correlationValue"]= correlation_value
                arryIndepVarObjects_dict["correlationObservationCount"]= obs_count
                arryIndepVarObjects_dict["definition"]= "The average temperature in a day of your location."
                arryIndepVarObjects_dict["noun"]= "daily average temperature"
                list_of_arryIndepVarObjects_dict.append(arryIndepVarObjects_dict)
            else:
                logger_ws_utilities.info(f"- No weather -")


        try:
            return list_of_arryIndepVarObjects_dict
        except:
            return []
    else:
        logger_ws_utilities.info(f"- User_id {user_id} has no df_qty_cat or df_workouts")
        return []

