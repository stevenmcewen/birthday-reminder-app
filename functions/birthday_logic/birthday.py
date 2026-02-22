# get birthdays for date range

from functions.sql.sql import SqlClient
from functions.logger.logger import get_logger
import pandas as pd
import datetime

logger = get_logger(__name__)

def get_daily_birthdays(*, sql_client: SqlClient) -> pd.DataFrame:
    """
    Function to get todays date and get daily birthdays from SQL database and return as a DataFrame.

    Args:
        sql_client (SqlClient): An instance of the SqlClient class to interact with the SQL database.
    Returns:
        pd.DataFrame: A DataFrame containing the names of people with birthdays today.
    """
    try:
        # get today's date
        date = datetime.datetime.now().date()
        # get birthdays for date from sql database
        name_dict_list = sql_client.get_birthdays_for_date(date)
        # convert to dataframe
        df = pd.DataFrame(name_dict_list, columns=["name", "email_to"])

        return df
    except Exception as e:
        logger.error("Error getting daily birthdays: %s", e)
        raise


def get_monthly_birthday_summary(*, sql_client: SqlClient) -> pd.DataFrame:
    """
    Function to get todays date and get monthly birthdays from SQL database and return as a DataFrame
    Args:
        sql_client (SqlClient): An instance of the SqlClient class to interact with the SQL database.  
    Returns:
        pd.DataFrame: A DataFrame containing the names and dates of birth of people with birthdays in the current month.
    """
    try:
        # get today's date
        date = datetime.datetime.now().date()
        # get birthdays for month from sql database
        birthday_dict_list = sql_client.get_birthdays_for_month(date)
        # convert to dataframe
        df = pd.DataFrame(birthday_dict_list, columns=["name", "birthday_day", "email_to"])

        return df
    except Exception as e:
        logger.error("Error getting monthly birthday summary: %s", e)
        raise