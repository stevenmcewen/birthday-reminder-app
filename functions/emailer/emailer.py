from __future__ import annotations

import pandas as pd

from azure.communication.email import EmailClient
from functions.config.config import get_settings
from functions.logger.logger import get_logger
from functions.sql.sql import SqlClient

logger = get_logger(__name__)
settings = get_settings()

# main functions
def send_monthly_birthday_summary_email(*, summary_df: pd.DataFrame) -> None:
    pass

def send_daily_birthday_emails(*, birthdays_df: pd.DataFrame) -> None:
    pass


# # helpers
# def send_prediction_email(payload: pd.DataFrame) -> None:
#     """
#     Send an email containing fixtures and predictions.
#     If there were no scoring predictions for the day, dont send any email.
#     """
#     # 1) parse the payload and extract all the email content
#     logger.info("Parsing Email information from dataframe scoring results search payload")
#     payload_df = parse_payload(payload)
#     subject, text_body, html_body, any_rows = build_email_bodies(payload_df)

#     # 2) Only do email processing if we have data to send via email
#     if not any_rows:
#         logger.info("There were no predictions today so no email will be sent")
#         return

#     # 3) Get email routing information
#     from_address = settings.email_from
#     to_addresses = parse_recipients(settings.email_to)
#     conn_str = settings.acs_email_connection_string

#     if not from_address or not to_addresses:
#         logger.warning("Email not sent: missing EMAIL_FROM and/or EMAIL_TO configuration.")
#         return
#     if not conn_str:
#         logger.warning("Email not sent: missing ACS_EMAIL_CONNECTION_STRING configuration.")
#         return

#     logger.info(
#         "Sending prediction email from=%r to=%r",
#         from_address,
#         to_addresses,
#     )

#     # 4) Build ACS message payload
#     message = {
#         "senderAddress": from_address,
#         "recipients": {"to": [{"address": addr} for addr in to_addresses]},
#         "content": {
#             "subject": subject,
#             "plainText": text_body,
#             "html": html_body,
#         },
#     }

#     #  5) Send via ACS SDK
#     try:
#         client = EmailClient.from_connection_string(conn_str)
#         poller = client.begin_send(message)
#         result = poller.result()

#         # result is a dictionary with 'id' key containing the message_id
#         message_id = result.get("id") if isinstance(result, dict) else getattr(result, "id", None)
#         logger.info("Prediction email sent via ACS. message_id=%s", message_id)

#     except Exception as exc:
#         logger.exception("Failed to send prediction email via ACS: %s", exc)
#         raise


