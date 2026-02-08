# get birthdays for date range

from functions.sql.sql import SqlClient
from functions.logger.logger import get_logger
import pandas as pd

logger = get_logger(__name__)

def get_daily_birthdays(*, sql_client: SqlClient) -> pd.DataFrame:
    pass

def get_monthly_birthday_summary(*, sql_client: SqlClient) -> pd.DataFrame:
    pass