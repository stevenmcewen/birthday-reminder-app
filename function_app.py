import json
import azure.functions as func

from functions.config.config import get_settings
from functions.sql.sql import SqlClient
from functions.logger.logger import get_logger

from functions.birthday_logic.birthday import get_monthly_birthday_summary, get_daily_birthdays
from functions.emailer.emailer import send_monthly_birthday_summary_email, send_daily_birthday_emails

app = func.FunctionApp()

logger = get_logger(__name__)
settings = get_settings()
sql_client = SqlClient(settings)


# Get monthly birthday summary
@app.schedule(
    schedule="0 0 5 1 * *", # 05:00 UTC on the first day of each month
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True,
)
def MonthlyBirthdaySummary(timer: func.TimerRequest) -> None:
    """
    Get monthly birthday summary.
    """

    logger.info("MonthlyBirthdaySummaryFunction triggered.")

    system_event = sql_client.start_system_event(
        function_name="MonthlyBirthdaySummaryFunction",
        trigger_type="timer",
        event_type="ingestion",
    )

    try:
        logger.info("Retrieving monthly birthday summary from SQL.")
        summary_df = get_monthly_birthday_summary(sql_client=sql_client, system_event_id=system_event.id)
        logger.info("Monthly birthday summary retrieved")

        logger.info("Sending monthly birthday summary email.")
        send_monthly_birthday_summary_email(summary_df=summary_df)
        logger.info("Monthly birthday summary email sent")

        sql_client.complete_system_event(
            system_event_id=system_event.id,
            status="succeeded",
        )
    except Exception as exc: 
        logger.exception("MonthlyBirthdaySummaryFunction failed.")
        sql_client.complete_system_event(
            system_event_id=system_event.id,
            status="failed",
            details=str(exc),
        )

# Get daily birthdays
@app.schedule(
    schedule="0 30 5 * * *", # 05:30 UTC every day
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True,
)
def DailyBirthdaySummary(timer: func.TimerRequest) -> None:
    """
    Get daily birthday summary.
    """

    logger.info("DailyBirthdaySummaryFunction triggered.")

    system_event = sql_client.start_system_event(
        function_name="DailyBirthdaySummaryFunction",
        trigger_type="timer",
        event_type="ingestion",
    )

    try:
        logger.info("Retrieving daily birthday summary from SQL.")
        summary_df = get_daily_birthdays(sql_client=sql_client, system_event_id=system_event.id)
        logger.info("Daily birthday summary retrieved")

        logger.info("Sending daily birthday summary email.")
        send_daily_birthday_emails(summary_df=summary_df)
        logger.info("Daily birthday summary email sent")

        sql_client.complete_system_event(
            system_event_id=system_event.id,
            status="succeeded",
        )
    except Exception as exc: 
        logger.exception("DailyBirthdaySummaryFunction failed.")
        sql_client.complete_system_event(
            system_event_id=system_event.id,
            status="failed",
            details=str(exc),
        )

# test functions
@app.route(route="test-monthly-summary", auth_level=func.AuthLevel.ANONYMOUS)
def MonthlyBirthdaySummaryTest(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get monthly birthday summary.
    """

    logger.info("MonthlyBirthdaySummaryFunction triggered.")

    system_event = sql_client.start_system_event(
        function_name="MonthlyBirthdaySummaryFunction",
        trigger_type="timer",
        event_type="ingestion",
    )

    try:
        logger.info("Retrieving monthly birthday summary from SQL.")
        summary_df = get_monthly_birthday_summary(sql_client=sql_client, system_event_id=system_event.id)
        logger.info("Monthly birthday summary retrieved")

        logger.info("Sending monthly birthday summary email.")
        send_monthly_birthday_summary_email(summary_df=summary_df)
        logger.info("Monthly birthday summary email sent")

        sql_client.complete_system_event(
            system_event_id=system_event.id,
            status="succeeded",
        )
    except Exception as exc: 
        logger.exception("MonthlyBirthdaySummaryFunction failed.")
        sql_client.complete_system_event(
            system_event_id=system_event.id,
            status="failed",
            details=str(exc),
        )
        return func.HttpResponse(
            json.dumps(
                {
                    "status": "error",
                    "message": "MonthlyBirthdaySummaryFunction failed",
                    "system_event_id": str(system_event.id),
                }
            ),
            status_code=500,
            mimetype="application/json",
        )  
        raise


@app.route(route="test-daily-summary", auth_level=func.AuthLevel.ANONYMOUS)
def DailyBirthdaySummaryTest(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get daily birthday summary.
    """

    logger.info("DailyBirthdaySummaryFunction triggered.")

    system_event = sql_client.start_system_event(
        function_name="DailyBirthdaySummaryFunction",
        trigger_type="timer",
        event_type="ingestion",
    )

    try:
        logger.info("Retrieving daily birthday summary from SQL.")
        summary_df = get_daily_birthdays(sql_client=sql_client, system_event_id=system_event.id)
        logger.info("Daily birthday summary retrieved")

        logger.info("Sending daily birthday summary email.")
        send_daily_birthday_emails(summary_df=summary_df)
        logger.info("Daily birthday summary email sent")

        sql_client.complete_system_event(
            system_event_id=system_event.id,
            status="succeeded",
        )
    except Exception as exc: 
        logger.exception("DailyBirthdaySummaryFunction failed.")
        sql_client.complete_system_event(
            system_event_id=system_event.id,
            status="failed",
            details=str(exc),
        )
        return func.HttpResponse(
            json.dumps(
                {
                    "status": "error",
                    "message": "DailyBirthdaySummaryFunction failed",
                    "system_event_id": str(system_event.id),
                }
            ),
            status_code=500,
            mimetype="application/json",
        )
        raise
