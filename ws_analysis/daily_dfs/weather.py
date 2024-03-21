from ..common.config_and_logger import config, logger_ws_analysis
import pandas as pd
from ws_models import engine, session_scope, WeatherHistory

def create_df_weather_history():
    with session_scope() as session:
        weather_hist_query = session.query(WeatherHistory)
    weather_hist_df = pd.read_sql(weather_hist_query.statement, engine)

    # Convert 'date_time' column from string to datetime
    weather_hist_df['date_time'] = pd.to_datetime(weather_hist_df['date_time'])
    # Extract the date component and overwrite the 'date_time' column
    weather_hist_df['date_time'] = weather_hist_df['date_time'].dt.date

    return weather_hist_df