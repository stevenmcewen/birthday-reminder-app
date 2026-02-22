from __future__ import annotations
import datetime

import pandas as pd

from azure.communication.email import EmailClient
from functions.config.config import get_settings
from functions.logger.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()

# main functions
def send_monthly_birthday_summary_emails(*, summary_df: pd.DataFrame) -> dict[str, int]:
    """
        Send a monthly summary email containing all birthdays for the month.

        Args:
            summary_df (pd.DataFrame): A dataframe containing the monthly birthday summary information.

        Returns:
            dict[str, int]: Counts for attempted/sent/failed emails.
    """
    # check if we have any data to send in the email, if not, skip email processing
    if summary_df.empty:
        logger.info("No birthday data for this month, skipping email processing.")
        return {"attempted": 0, "sent": 0, "failed": 0}

    logger.info("Parsing monthly summary information from dataframe")
    # parse the summary dataframe to extract relevant information
    payload_df, to_addresses = parse_payload(summary_df)
    result = {"attempted": len(to_addresses), "sent": 0, "failed": 0}

    for to_address in to_addresses:
        logger.info("Building and sending monthly birthday summary email to %s", to_address)
        try:
            # build email subject, text body and html body from the parsed information
            subject, text_body, html_body = build_email_bodies_monthly(payload_df, to_address)

            # send email via ACS
            send_email(subject, text_body, html_body, to_address)
        except Exception as exc:
            result["failed"] += 1
            logger.exception("Failed to send monthly birthday summary email to %s: %s", to_address, exc)
            continue
        else:
            result["sent"] += 1

    return result


def send_daily_birthday_emails(*, summary_df: pd.DataFrame) -> dict[str, int]:
    """
        Send daily birthday emails containing all birthdays for the day.

        Args:
            summary_df (pd.DataFrame): A dataframe containing the daily birthday information.

        Returns:
            dict[str, int]: Counts for attempted/sent/failed emails.
    """
    # check if we have any data to send in the email, if not, skip email processing
    if summary_df.empty:
        logger.info("No birthday data for today, skipping email processing.")
        return {"attempted": 0, "sent": 0, "failed": 0}
    logger.info("Parsing daily birthday information from dataframe")
    # parse the summary dataframe to extract relevant information
    payload_df, to_addresses = parse_payload(summary_df)
    result = {"attempted": len(to_addresses), "sent": 0, "failed": 0}

    for to_address in to_addresses:
        logger.info("Building and sending daily birthday email to %s", to_address)
        try:
            # build email subject, text body and html body from the parsed information
            subject, text_body, html_body = build_email_bodies_daily(payload_df, to_address)

            # send email via ACS
            send_email(subject, text_body, html_body, to_address)
        except Exception as exc:
            result["failed"] += 1
            logger.exception("Failed to send daily birthday email to %s: %s", to_address, exc)
            continue
        else:
            result["sent"] += 1

    return result
    

# helpers
# parser functions to extract relevant information from the summary dataframe for email generation
def parse_payload(summary_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Parse the summary dataframe to extract relevant information for email generation.

    Args:
        summary_df (pd.DataFrame): A dataframe containing the birthday summary information.
    Returns:
        tuple[pd.DataFrame, list[str]]: A tuple containing the parsed dataframe for email generation and a list of recipient email addresses.
    """
    # extract the email addresses from the dataframe, dropping duplicates and nulls
    to_addresses = summary_df["email_to"].dropna().unique().tolist()

    # for simplicity, we will pass the entire dataframe to the email body builders and let them handle the formatting
    payload_df = summary_df

    return payload_df, to_addresses

# email body builders to construct the email subject, text body and html body for both daily and monthly emails
def build_email_bodies_monthly(payload_df: pd.DataFrame, to_address: str) -> tuple[str, str, str]:
    """
    Build the email subject, text body and html body for the monthly birthday summary email.

    Args:
        payload_df (pd.DataFrame): A dataframe containing the parsed birthday summary information for the month.
        to_address (str): The recipient's email address.
    Returns:
        tuple[str, str, str]: A tuple containing the email subject, text body and html body for the monthly birthday summary email.
    """
    month = datetime.datetime.now().strftime("%B")
    subject = f"{month} Birthday Summary"
    text_body = f"Here is the monthly birthday summary for {month}:\n\n"
    html_body = f"<html><body><h1>{month} Birthday Summary</h1><ul>"

    # filter dataframe for only rows relevant to this email address
    email_df = payload_df[payload_df["email_to"] == to_address]

    for _, row in email_df.iterrows():
        name = row.get("name", "Unknown")
        birthday = row.get("birthday_day", "Unknown")
        text_body += f"- {name}: {birthday}\n"
        html_body += f"<li>{name}: {birthday}</li>"

    html_body += "</ul></body></html>"

    return subject, text_body, html_body

def build_email_bodies_daily(payload_df: pd.DataFrame, to_address: str) -> tuple[str, str, str]:
    """
    Build the email subject, text body and html body for the daily birthday email.

    Args:
        payload_df (pd.DataFrame): A dataframe containing the parsed birthday information for the day.
        to_address (str): The recipient's email address.
    Returns:
        tuple[str, str, str]: A tuple containing the email subject, text body and html body for the daily birthday email.
    """
    today = datetime.datetime.now().strftime("%B %d")
    subject = f"Birthdays for {today}"
    text_body = f"Here are the birthdays for today ({today}):\n\n"
    html_body = f"<html><body><h1>Birthdays for {today}</h1><ul>"

    # filter dataframe for only rows relevant to this email address
    email_df = payload_df[payload_df["email_to"] == to_address]

    for _, row in email_df.iterrows():
        name = row.get("name", "Unknown")
        text_body += f"- {name}\n"
        html_body += f"<li>{name}</li>"

    html_body += "</ul></body></html>"

    return subject, text_body, html_body

# email sender
def send_email(subject: str, text_body: str, html_body: str, to_address: str) -> None:
    """
    Send an email containing the provided subject, text body and html body to the specified email address using Azure Communication Services (ACS).

    Args:
        subject (str): The subject of the email.
        text_body (str): The plain text body of the email.
        html_body (str): The HTML body of the email.
        to_address (str): The recipient's email address.

    Returns:
        None
    """

    # 1) Get email routing information
    from_address = settings.email_from
    conn_str = settings.acs_email_connection_string

    if not conn_str:
        logger.warning("Email not sent: missing ACS_EMAIL_CONNECTION_STRING configuration.")
        return
    
    if not from_address:
        logger.warning("Email not sent: missing EMAIL_FROM configuration.")
        return

    logger.info(
        "Sending email from=%r to=%r",
        from_address,
        to_address,
    )

    # 4) Build ACS message payload
    message = {
        "senderAddress": from_address,
        "recipients": {"to": [{"address": to_address}]},
        "content": {
            "subject": subject,
            "plainText": text_body,
            "html": html_body,
        },
    }

    #  5) Send via ACS SDK
    try:
        client = EmailClient.from_connection_string(conn_str)
        poller = client.begin_send(message)
        result = poller.result()

        # result is a dictionary with 'id' key containing the message_id
        message_id = result.get("id") if isinstance(result, dict) else getattr(result, "id", None)
        logger.info("Birthday email sent via ACS. message_id=%s", message_id)

    except Exception as exc:
        logger.exception("Failed to send birthday email via ACS: %s", exc)
        raise


