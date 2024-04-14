import json
import requests
from datetime import datetime, timedelta
import os
import pandas as pd
import time
from ws_models import DatabaseSession, Users, WeatherHistory, Locations, UserLocationDay
from ..common.config_and_logger import config, logger_ws_utilities
from ..common.utilities import wrap_up_session
from ..visual_crossing_api.vc_api_requests import request_visual_crossing_for_one_day


def collect_yesterday_weather_history_from_visual_crossing():
    ###########################################################################################
    # This function updates the weather history for yesterday for all locations in Locations ##
    ###########################################################################################
    db_session = DatabaseSession()
    
    yesterday_date = datetime.utcnow()  - timedelta(days=1)
    yesterday_date_str = yesterday_date.strftime('%Y-%m-%d')
    date_1_start = yesterday_date_str
    logger_ws_utilities.info(f"- Collecting Weather History for: {date_1_start} -")
    locations_list = db_session.query(Locations).all()
    weather_hist_call_counter = 0
    for location in locations_list:
        logger_ws_utilities.info(f"- Checking Weather History for: {location.id} - {location.city}, {location.country} -")
        weather_hist_exists = db_session.query(WeatherHistory).filter_by(
            location_id = location.id).filter_by(date_time=yesterday_date_str).first()
        
        if not weather_hist_exists:
            logger_ws_utilities.info(f"- Weather History on {date_1_start} does not exist for: {location.id}  -")
            weather_hist_call_counter += 1
            weather_data = request_visual_crossing_for_one_day(location, date_1_start)
            if len(weather_data) >0:
                add_weather_history(db_session, location.id, weather_data)

    logger_ws_utilities.info(f"- Made {weather_hist_call_counter} VC API calls for weather history locations ")
    wrap_up_session(db_session)


# def interpolate_missing_dates_exclude_references(df):
#     # Ensure the DataFrame is sorted by date
#     df.sort_values(by='date_utc_user_check_in', inplace=True)
    
#     # Initialize a list to hold the interpolated rows
#     interpolated_rows = []
    
#     # Get the current UTC time to use for all interpolated rows
#     current_utc_time = datetime.utcnow()
    
#     # Iterate through the DataFrame to find gaps and create interpolated rows
#     for i in range(len(df) - 1):
#         current_row = df.iloc[i]
#         next_row = df.iloc[i + 1]
        
#         current_date = current_row['date_utc_user_check_in']
#         next_date = next_row['date_utc_user_check_in']
        
#         # Calculate the gap in days between current and next date
#         gap_days = (next_date - current_date).days
        
#         # If there is a gap, create the necessary interpolated rows
#         for gap_day in range(1, gap_days):
#             interpolated_date = current_date + pd.Timedelta(days=gap_day)
#             interpolated_row = {
#                 'user_id': current_row['user_id'],
#                 'location_id': current_row['location_id'],
#                 'date_utc_user_check_in': interpolated_date,
#                 'row_type': 'interpolated',
#                 'date_time_utc_user_check_in': current_utc_time,
#                 'time_stamp_utc': current_utc_time
#                 # Fill other columns as needed, e.g., set to None or default values
#             }
#             interpolated_rows.append(interpolated_row)
    
#     # Convert the list of dictionaries to a DataFrame
#     interpolated_df = pd.DataFrame(interpolated_rows)
    
#     return interpolated_df


def add_weather_history(db_session, location_id, weather_data):

    logger_ws_utilities.info(f"-- in add_weather_history")
    logger_ws_utilities.info(f"-- location_id: {location_id} --")
    # db_session = DatabaseSession()

    for day in weather_data['days']:

        # Check weather record does not already exist
        weather_hist_exists = db_session.query(WeatherHistory).filter_by(
            location_id = location_id).filter_by(date_time=day['datetime']).first()
        
        if not weather_hist_exists:

            weather_history = WeatherHistory(
                location_id=location_id,
                date_time=day['datetime'],
                datetimeEpoch=day['datetimeEpoch'],
                tempmax=day.get('tempmax'),
                tempmin=day.get('tempmin'),
                temp=day.get('temp'),
                feelslikemax=day.get('feelslikemax'),
                feelslikemin=day.get('feelslikemin'),
                feelslike=day.get('feelslike'),
                dew=str(day.get('dew')),
                humidity=str(day.get('humidity')),
                precip=str(day.get('precip')),
                precipprob=str(day.get('precipprob')),
                precipcover=str(day.get('precipcover')),
                preciptype=str(day.get('preciptype')),
                snow=str(day.get('snow')),
                snowdepth=str(day.get('snowdepth')),
                windgust=str(day.get('windgust')),
                windspeed=str(day.get('windspeed')),
                winddir=str(day.get('winddir')),
                pressure=str(day.get('pressure')),
                cloudcover=str(day.get('cloudcover')),
                visibility=str(day.get('visibility')),
                solarradiation=str(day.get('solarradiation')),
                solarenergy=str(day.get('solarenergy')),
                uvindex=str(day.get('uvindex')),
                sunrise=day.get('sunrise'),
                sunriseEpoch=str(day.get('sunriseEpoch')),
                sunset=day.get('sunset'),
                sunsetEpoch=str(day.get('sunsetEpoch')),
                moonphase=str(day.get('moonphase')),
                conditions=day.get('conditions'),
                description=day.get('description'),
                icon=day.get('icon'),
                unitgroup=config.VISUAL_CROSSING_UNIT_GROUP,
                time_stamp_utc=datetime.utcnow()
            )
            db_session.add(weather_history)
            logger_ws_utilities.info(f"weather_history: {weather_history}")
        else:
            long_f_string = (
                f"weather_history for location_id:{location_id}" +
                f" on date: {day['datetime']} already exists"
            )
            logger_ws_utilities.info(long_f_string)
    # Commit the session to save these objects to the database
    # wrap_up_session(db_session)


