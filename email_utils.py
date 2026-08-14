"""
email_utils.py
===============
Helpers for building HTML email bodies and sending notification
emails over SMTP.

Two things live here:

* :func:`build_status_html_table` - turns a Spark DataFrame of
  workflow statuses into a color-coded HTML table (green = success,
  orange = failed) suitable for embedding in an email body.
* :func:`send_email_smtp` - sends an HTML email via SMTP (e.g. Office
  365 / Outlook).

Credentials are never hardcoded; see :mod:`config` for how the SMTP
password is resolved from a Databricks secret scope or environment
variable.
"""

from __future__ import annotations

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Iterable, Optional

# `pyspark.sql.DataFrame` is only imported for type hints; the module
# works fine without a live Spark session for testing purposes.
try:
    from pyspark.sql import DataFrame as SparkDataFrame
except ImportError:  # pragma: no cover - allows import outside Databricks
    SparkDataFrame = None  # type: ignore


_TABLE_CSS = """
<style>
#tableformatting table, table th, table td {
    font-size: 10pt;
    border: 1px solid black;
    border-collapse: collapse;
    text-align: left;
    padding: 5px;
}
thead { background-color: yellow; }
table { border-collapse: collapse; }
.success { background-color: green; }
.fail { background-color: Orange; }
</style>
"""

# Values that should be rendered in the "success" (green) style.
_SUCCESS_VALUES = {"SUCCESS", "Y", "COMPLETED"}
# Values that should be rendered in the "fail" (orange) style.
_FAIL_VALUES = {"FAILED", "N"}


def _render_html_table(df) -> str:
    """Render a Spark DataFrame as a color-coded HTML ``<table>``.

    Args:
        df: A Spark DataFrame whose columns/rows will become the
            table header/body.

    Returns:
        An HTML string containing the ``<table>`` markup (CSS
        excluded - see :data:`_TABLE_CSS`).
    """
    html = "<br><br><table border='1'><thead><tr>"
    for column_name in df.columns:
        html += f"<th>{column_name}</th>"
    html += "</tr></thead><tbody>"

    for row in df.collect():
        html += "<tr>"
        for value in row:
            display_value = str(value).strip().upper() if value is not None else ""
            if display_value == "NOT STARTED":
                html += f"<td class='not-started'>{value}</td>"
            elif display_value in _FAIL_VALUES:
                html += f"<td class='fail'>{value}</td>"
            elif display_value in _SUCCESS_VALUES or "COMPLETED" in display_value:
                html += f"<td class='success'>{value}</td>"
            else:
                html += f"<td>{value}</td>"
        html += "</tr>"
    html += "</tbody></table>"
    return html


def build_status_html_table(
    email_intro: str,
    df_body,
    sftp_df=None,
    signature: str = "Team Data Integration",
) -> str:
    """Build a full HTML email body with a workflow status table.

    Args:
        email_intro: Introductory HTML/text placed above the table
            (e.g. a greeting and summary sentence).
        df_body: Spark DataFrame with the primary workflow status
            rows to render.
        sftp_df: Optional second Spark DataFrame (e.g. SFTP workflow
            status) rendered as a second table below the first.
        signature: Closing signature line appended to the email.

    Returns:
        A complete HTML string ready to be used as an email body.
    """
    html = email_intro + _TABLE_CSS + _render_html_table(df_body)

    if sftp_df is not None:
        html += "<br><br>SFTP Workflow Status"
        html += _TABLE_CSS + _render_html_table(sftp_df)

    html += f"<br><br>Thank you,<br>{signature}"
    return html


def send_email_smtp(
    smtp_server: str,
    smtp_port: int,
    sender_email: str,
    sender_password: Optional[str],
    send_to: Iterable[str],
    subject: str,
    html_body: str,
    send_cc: Optional[Iterable[str]] = None,
) -> bool:
    """Send an HTML email over SMTP with STARTTLS.

    Args:
        smtp_server: SMTP server hostname, e.g. ``"smtp.office365.com"``.
        smtp_port: SMTP server port, typically ``587`` for STARTTLS.
        sender_email: The "From" address and SMTP login username.
        sender_password: The SMTP account password/app-password. Must
            be supplied by the caller (see :mod:`config` for how to
            resolve it securely) - never hardcode this value.
        send_to: Iterable of recipient email addresses.
        subject: Email subject line.
        html_body: Pre-built HTML email body (see
            :func:`build_status_html_table`).
        send_cc: Optional iterable of CC email addresses.

    Returns:
        ``True`` if the email was sent successfully, ``False`` otherwise.
    """
    if not sender_password:
        raise ValueError(
            "SMTP password is not configured. Set it via a Databricks "
            "secret scope or the SMTP_PASSWORD environment variable."
        )

    send_to = list(send_to)
    send_cc = list(send_cc) if send_cc else []

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(send_to)
    if send_cc:
        msg["Cc"] = ", ".join(send_cc)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, send_to + send_cc, msg.as_string())
        server.quit()
        print(f"Email sent successfully to: {send_to}")
        return True
    except Exception as exc:  # noqa: BLE001 - surface any SMTP failure
        print(f"Email sending failed: {exc}")
        return False
