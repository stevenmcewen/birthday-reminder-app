from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from uuid import UUID
import time
import sqlalchemy as sa
from azure.identity import DefaultAzureCredential
import pyodbc
import struct 

from functions.config.config import AppSettings
from functions.logger.logger import get_logger

logger = get_logger(__name__)

# system event class ###
@dataclass
class SystemEvent:
    """
    Lightweight representation of a row in dbo.system_events.

    This object is returned by the SQL client when a new system event
    is started, so callers can propagate the event id into downstream
    logic (e.g. ingestion_events).
    """

    id: UUID


class SqlClient:
    """
    Thin wrapper around Azure SQL access for system / ingestion metadata.

    Responsibilities:
    - Manage connections to Azure SQL (using managed identity in production).
    - Provide high-level methods for:
      - Starting / completing rows in dbo.system_events.
    """

    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.credential = DefaultAzureCredential()

        # Build a raw ODBC connection string. We use a custom creator
        # so that each new connection gets a *fresh* AAD token.

        logger.info("SQL config - server=%s, database=%s", settings.sql_server, settings.sql_database)

        odbc_conn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server=tcp:{settings.sql_server},1433;"
            f"Database={settings.sql_database};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )

        def _get_connection():
            """
            Factory used by SQLAlchemy for each new DBAPI connection.

            This ensures we always attach a current access token and
            avoid token‑expiry issues with long‑lived pools. It also
            adds some basic retry logic to improve resilience during
            cold starts (e.g. when SQL or managed identity is still
            waking up).
            """
            max_attempts = 3
            delay_seconds = 5
            last_exc: Exception | None = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return pyodbc.connect(
                        odbc_conn_str,
                        attrs_before=self.get_token(),
                    )
                except Exception as exc:
                    last_exc = exc
                    logger.warning(
                        "SQL connection attempt %s/%s failed: %s",
                        attempt,
                        max_attempts,
                        exc,
                    )
                    if attempt < max_attempts:
                        time.sleep(delay_seconds)
                        # exponential backoff pattern
                        delay_seconds *= 2

            logger.error("SQL connection failed after %s attempts", max_attempts)
            raise last_exc

        # fast_executemany is set to False to avoid the issue of the SQL Server automatically setting incorrect string max lengths,
        # resulting in buffer overflow errors.
        self.engine = sa.create_engine(
            "mssql+pyodbc://",
            creator=_get_connection,
            pool_pre_ping=True,
            fast_executemany=False,
        )

        logger.info(
            "SqlClient connected: env=%s server=%s db=%s",
            settings.environment,
            settings.sql_server,
            settings.sql_database,
        )
    
    ## connection helpers ###
    def get_token(self) -> dict:
        """
        Get an Azure AD access token for the SQL Server.

        returns a dictionary that can be passed to the connect_args parameter of the SQL Alchemy engine.
        """
        token = self.credential.get_token("https://database.windows.net/.default").token
        # ODBC driver expects raw binary format
        token_bytes = token.encode("utf-16-le")
        exptoken = struct.pack("=i", len(token_bytes)) + token_bytes
        token = {1256: exptoken}
        return token
    
    ### sql methods ###

    ## event helpers ###
    def start_system_event(
        self,
        *,
        function_name: str,
        trigger_type: str,
        event_type: str,
        status: str = "started",
        details: str|None = None,
    ) -> SystemEvent:
        """
        Insert a new row into dbo.system_events and return its id.

        Intended usage pattern:
        - Call at the very start of an Azure Function.
        - Capture the returned id and pass it into downstream services
          (e.g. ingestion) so they can link ingestion_events to it.
        """
        try: 
            with self.engine.connect() as conn:
                result = conn.execute(sa.text("""
                    INSERT INTO dbo.system_events (function_name, trigger_type, event_type, status, details)
                    OUTPUT INSERTED.id
                    VALUES (:function_name, :trigger_type, :event_type, :status, :details);"""
                    ), 
                    {
                    "function_name": function_name,
                    "trigger_type": trigger_type,
                    "event_type": event_type,
                    "status": status,
                    "details": details,
                }
                )
                row = result.first()
                conn.commit()
                event_id = UUID(str(row[0]))
                logger.info(
                    "Starting system_event id=%s function=%s trigger=%s type=%s status=%s",
                    event_id,
                    function_name,
                    trigger_type,
                    event_type,
                    status,
                )
                return SystemEvent(id=event_id)
        except Exception as e:
            logger.error("Error starting system event: %s", e)
            raise


    def complete_system_event(
        self,
        *,
        system_event_id: UUID,
        status: str,
        details: str|None = None,
    ) -> None:
        """
        Mark an existing dbo.system_events row as completed (success / failure) at the end of a function run.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(sa.text("""
                    UPDATE dbo.system_events
                    SET status = :status,
                    completed_at = SYSUTCDATETIME(),
                    details = :details
                    WHERE id = :system_event_id;"""
                    ), 
                    {
                        "status": status,
                        "details": details,
                        "system_event_id": system_event_id,
                    }
                )
                conn.commit()
                logger.info(
                    "Completing system_event id=%s with status=%s",
                    system_event_id,
                    status,
                )
                if details:
                    logger.info("system_event id=%s completion details=%s", system_event_id, details)
        except Exception as e:
            logger.error("Error completing system event: %s", e)
            raise

    ## Get birthdays for date
    def get_birthdays_for_date(self, date) -> list[dict]:
        """
        Retrieve birthdays that fall on the same month/day as the provided date.
        Returns a dictionary in the format: [{"name": "...", "email_to": "..."}]
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    sa.text("""
                        SELECT FullName, EmailTo
                        FROM dbo.Birthdays
                        WHERE MONTH(date_of_birth) = MONTH(:date)
                        AND DAY(date_of_birth)   = DAY(:date)
                        ORDER BY EmailTo, FullName;
                    """),
                    {"date": date},
                )

                birthdays = [{"name": row.FullName, "email_to": row.EmailTo} for row in result]
                logger.info("Retrieved %s birthdays for date %s", len(birthdays), date)
                return birthdays

        except Exception as e:
            logger.error("Error retrieving birthdays for date %s: %s", date, e)
            raise

    
    def get_birthdays_for_month(self, date) -> list[dict]:
        """
        Retrieve birthdays that fall within the month of the provided date.
        Returns: [{"name": "...", "birthday_day": <int>, "email_to": "..."}]
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    sa.text("""
                        SELECT FullName, DAY(date_of_birth) as birthday_day, EmailTo
                        FROM dbo.Birthdays
                        WHERE MONTH(date_of_birth) = MONTH(:date)
                        ORDER BY EmailTo, DAY(date_of_birth), FullName;
                    """),
                    {"date": date},
                )

                birthdays = [{"name": row.FullName, "birthday_day": row.birthday_day, "email_to": row.EmailTo} for row in result]
                logger.info("Retrieved %s birthdays for month of date %s", len(birthdays), date)
                return birthdays

        except Exception as e:
            logger.error("Error retrieving birthdays for month of date %s: %s", date, e)
            raise
    