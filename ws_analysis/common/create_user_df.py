from .config_and_logger import config, logger_ws_analysis, wrap_up_session
from .utilities import convert_to_user_tz, get_dateUserTz_3pm, \
    calculate_duration_in_hours, create_pickle_apple_qty_cat_path_and_name, \
    create_pickle_apple_workouts_path_and_name, \
    adjust_timezone, add_timezones_from_UserLocationDay
import pandas as pd
# from ws_models import engine, sess, Users, UserLocationDay, Locations
from ws_models import engine, DatabaseSession, Users, UserLocationDay, Locations
from datetime import datetime
import pytz
import os

##################################################################
# This creates user unique df's used to calculate corrloations ###
# These contain the entirty of a user's data in WS.            ###
##################################################################


# def create_user_qty_cat_df(user_id, user_tz_str='Europe/Paris'):
def create_user_qty_cat_df(user_id):
    # Query data from database into pandas dataframe

    logger_ws_analysis.info("- in create_user_qty_cat_df -")
    db_session = DatabaseSession()

    pickle_apple_qty_cat_path_and_name = create_pickle_apple_qty_cat_path_and_name(user_id)
    if os.path.exists(pickle_apple_qty_cat_path_and_name):
        logger_ws_analysis.info(f"- reading pickle file for workouts: {pickle_apple_qty_cat_path_and_name} -")
        # df_existing_user_workouts_data=pd.read_pickle(pickle_apple_qty_cat_path_and_name)
        df=pd.read_pickle(pickle_apple_qty_cat_path_and_name)
    else:
        query = f"SELECT * FROM apple_health_quantity_category WHERE user_id = {user_id}"
        df = pd.read_sql_query(query, engine)
    logger_ws_analysis.info(f"user_id: {user_id}")
    # user_tz_str = sess.get(Users,user_id).timezone
    user_tz_str = db_session.get(Users,user_id).timezone
    wrap_up_session(db_session)
    
    if user_tz_str == None or user_tz_str == "":
        logger_ws_analysis.info(f"- Error: ws_analysis/common/create_user_df.py/create_user_qty_cat_df --> user_tz_str is None or blank -")
    else:
        logger_ws_analysis.info(f"user_tz_str: {user_tz_str}")
    
    # Create new column called user_tz_str 
    df['date_utc'] = df['startDate'].str[:10]
    df['user_tz_str'] = user_tz_str

    # Remove the +0000 from end of all utc strings from startDate and endDate
    df['startDate'] = df['startDate'].str[:-6]
    df['endDate'] = df['endDate'].str[:-6]

    df = add_timezones_from_UserLocationDay(user_id, df)
    


    try:
        # Create new columns startDateUserTz and endDateUserTz
        # Apply the adjust_timezone function for startDate and endDate
        df['startDateUserTz'] = df.apply(lambda row: adjust_timezone(row['startDate'], row['user_tz_str']), axis=1)
        df['endDateUserTz'] = df.apply(lambda row: adjust_timezone(row['endDate'], row['user_tz_str']), axis=1)


        # df.to_csv(os.path.join(config.PROJECT_RESOURCES, "create_user_df_Tz_step1.csv"))
        # df.to_pickle(os.path.join(config.PROJECT_RESOURCES, "create_user_df_Tz_step1.pkl"))


        # Create a temporary column for datetime conversion
        try:
            df['startDateUserTz_temp'] = pd.to_datetime(df['startDateUserTz'])
        except AttributeError as e:
            df['startDateUserTz_temp'] = pd.to_datetime(df['startDateUserTz'], errors='coerce')
            logger_ws_analysis.info(f"---- failed to converted some startDateUserTz , error: {e}")

        # df.startDateUserTz = df.startDateUserTz.astype(str)
        # # Use the temporary column to extract the date part and create 'dateUserTz'
        df['dateUserTz'] = df['startDateUserTz_temp'].dt.date


        # Drop the temporary column
        df.drop('startDateUserTz_temp', axis=1, inplace=True)

        list_of_user_data = list(df.sampleType.unique())
        # df.to_csv(os.path.join(config.PROJECT_RESOURCES, "create_user_df_adjusted.csv"))
        # df.to_pickle(os.path.join(config.PROJECT_RESOURCES, "create_user_df_adjusted.pkl"))
        # logger_ws_analysis.info(f"- success: created: {os.path.join(config.PROJECT_RESOURCES, 'create_user_df_adjusted.csv')} DELETE-")
        return df, list_of_user_data

    except Exception as e:
        logger_ws_analysis.info("* User probably has NO Apple Quantity or Category Data *")
        logger_ws_analysis.info(f"An error occurred (in send_data_source_objects): {e}")
        return "insufficient data", "insufficient data"
    


def create_user_workouts_df(user_id):
    logger_ws_analysis.info("- in create_user_workouts_df -")
    # Query data from database into pandas dataframe
    # user_id=user_id
    pickle_apple_workouts_path_and_name = create_pickle_apple_workouts_path_and_name(user_id)
    if os.path.exists(pickle_apple_workouts_path_and_name):
        logger_ws_analysis.info(f"- reading pickle file for workouts: {pickle_apple_workouts_path_and_name} -")
        # df_existing_user_workouts_data=pd.read_pickle(pickle_apple_workouts_path_and_name)
        df=pd.read_pickle(pickle_apple_workouts_path_and_name)
    else:
        query = f"SELECT * FROM apple_health_workout WHERE user_id = {user_id}"
        df = pd.read_sql_query(query, engine)
    
    user_tz_str = sess.get(Users,user_id).timezone
    if user_tz_str == None or user_tz_str == "":
        logger_ws_analysis.info(f"- Error: ws_analysis/common/create_user_df.py/create_user_qty_cat_df --> user_tz_str is None or blank -")
    
    ## Start NEW METHOD ##
    # Create new column called user_tz_str 
    df['date_utc'] = df['startDate'].str[:10]
    df['user_tz_str'] = user_tz_str

    # Remove the +0000 from end of all utc strings from startDate and endDate
    df['startDate'] = df['startDate'].str[:-6]
    df['endDate'] = df['endDate'].str[:-6]

    df = add_timezones_from_UserLocationDay(user_id, df)

    try:
        # Create new columns startDateUserTz and endDateUserTz
        # Apply the adjust_timezone function for startDate and endDate
        df['startDateUserTz'] = df.apply(lambda row: adjust_timezone(row['startDate'], row['user_tz_str']), axis=1)
        df['endDateUserTz'] = df.apply(lambda row: adjust_timezone(row['endDate'], row['user_tz_str']), axis=1)

        # Create a temporary column for datetime conversion
        try:
            df['startDateUserTz_temp'] = pd.to_datetime(df['startDateUserTz'])
        except AttributeError as e:
            df['startDateUserTz_temp'] = pd.to_datetime(df['startDateUserTz'], errors='coerce')
            logger_ws_analysis.info(f"---- failed to converted some startDateUserTz , error: {e}")

        # df.startDateUserTz = df.startDateUserTz.astype(str)
        # # Use the temporary column to extract the date part and create 'dateUserTz'
        df['dateUserTz'] = df['startDateUserTz_temp'].dt.date

        # Drop the temporary column
        df.drop('startDateUserTz_temp', axis=1, inplace=True)

        list_of_user_data = list(df.sampleType.unique())
        # df.to_csv(os.path.join(config.PROJECT_RESOURCES, "create_user_df_workouts_adjusted.csv"))
        # logger_ws_analysis.info(f"- success: created: {os.path.join(config.PROJECT_RESOURCES, 'create_user_df_workouts_adjusted.csv')} DELETE-")
        return df, list_of_user_data

    except Exception as e:
        logger_ws_analysis.info("* User probably has NO Apple Quantity or Category Data *")
        logger_ws_analysis.info(f"An error occurred (in send_data_source_objects): {e}")
        return "insufficient data", "insufficient data"
    

def create_user_location_date_df(user_id):
    logger_ws_analysis.info("- in create_user_location_date_df")
    user_locations_day_query = sess.query(UserLocationDay).filter_by(user_id = user_id)
    user_locations_day_df = pd.read_sql(user_locations_day_query.statement, engine)
    user_locations_day_df.rename(columns={'date_utc_user_check_in': 'date'},inplace=True)

    return user_locations_day_df[['date','location_id']]

