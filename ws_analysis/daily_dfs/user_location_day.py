from ..common.config_and_logger import config, logger_ws_analysis
import pandas as pd
import datetime


def create_df_daily_user_location_consecutive(start_date, end_date, df_daily_user_location):
    logger_ws_analysis.info("- in create_df_daily_user_location_consecutive -")
    search_row_date = start_date
    df = pd.DataFrame()
    while search_row_date <= end_date:
    
        df_next_row = return_next_day(df_daily_user_location, search_row_date)
        df_next_row.reset_index(inplace=True, drop=True)

        if df_next_row.date.loc[0] == search_row_date:
            # Use pd.concat to add the new row to the existing DataFrame
            df = pd.concat([df, df_next_row], ignore_index=True)
        else:
            location_id = df_next_row.location_id.loc[0]
            df_next_row = pd.DataFrame({'date': [search_row_date], 'location_id': [location_id]})
            df = pd.concat([df, df_next_row], ignore_index=True)

        search_row_date = search_row_date + datetime.timedelta(days=1)
    return df

################
# Utility #
###############

def return_next_day(df_daily_user_location, search_row_date):
    # Attempt to find a row that matches the search date exactly
    df = df_daily_user_location[df_daily_user_location['date'] == search_row_date]
    if len(df) == 0:
        # Filter the DataFrame for rows with dates older than the search date
        older_dates_df = df_daily_user_location[df_daily_user_location['date'] < search_row_date]
        # Filter the DataFrame for rows with dates newer than the search date
        newer_dates_df = df_daily_user_location[df_daily_user_location['date'] > search_row_date]
        if not older_dates_df.empty:
            # Find the maximum date within the filtered DataFrame for older dates
            closest_older_date = older_dates_df['date'].max()
            # Retrieve the row with the closest older date
            df = df_daily_user_location[df_daily_user_location['date'] == closest_older_date]
            # logger_ws_analysis.info(f"Row with the closest older date to {search_row_date} :\n {df}")
        elif not newer_dates_df.empty:
            # Find the minimum date within the filtered DataFrame for newer dates
            closest_newer_date = newer_dates_df['date'].min()
            # Retrieve the row with the closest newer date
            df = df_daily_user_location[df_daily_user_location['date'] == closest_newer_date]
            # logger_ws_analysis.info(f"Row with the closest newer date to {search_row_date} :\n {df}")
        else:
            # If no older or newer dates are found, return an empty DataFrame with the same columns
            # logger_ws_analysis.info("No matching rows found for the specified criteria in the DataFrame.")
            df = pd.DataFrame(columns=df_daily_user_location.columns)
    return df
