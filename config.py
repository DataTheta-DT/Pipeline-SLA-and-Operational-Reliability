"""
config.py
=========
Central configuration for the Job Monitoring solution.

This module holds every value that changes between environments
(workspace URL, credentials, email recipient lists, SLA thresholds).
No secrets are hardcoded here. All secrets are read at runtime from
either Databricks Secret Scopes (preferred, when running inside
Databricks) or from environment variables (useful for local testing
or CI/CD pipelines).

SECURITY NOTE
-------------
The original version of this project had a live Databricks Personal
Access Token and an email account password hardcoded directly in the
source file. That is a serious security risk (secrets in source
control / notebooks can leak via git history, exports, or screen
shares). Both values have been removed and replaced with secret
lookups. If those old credentials were ever committed anywhere,
rotate/revoke them.

How to configure secrets in Databricks
---------------------------------------
1. Create a secret scope (one time):
     databricks secrets create-scope job-monitoring
2. Add the secrets:
     databricks secrets put-secret job-monitoring databricks-pat
     databricks secrets put-secret job-monitoring smtp-password
3. This module will automatically read them via `dbutils.secrets.get`
   when running inside a Databricks notebook/job.

How to configure secrets locally (outside Databricks)
-------------------------------------------------------
Set environment variables before running:
     export DATABRICKS_ADMIN_PAT="your-token"
     export SMTP_PASSWORD="your-app-password"
"""

from __future__ import annotations

import os
from enum import IntEnum, unique
from typing import Optional


def _get_secret(secret_key: str, env_var_name: str, scope: str = "job-monitoring") -> Optional[str]:
    """Retrieve a secret value, preferring Databricks Secret Scopes.

    Falls back to an environment variable if Databricks utilities
    (``dbutils``) are not available (e.g. when running/testing this
    code outside of a Databricks notebook).

    Args:
        secret_key: The key name of the secret inside the Databricks
            secret scope.
        env_var_name: The environment variable name to fall back to.
        scope: The Databricks secret scope name. Defaults to
            ``"job-monitoring"``.

    Returns:
        The secret value as a string, or ``None`` if it could not be
        resolved from either source.
    """
    try:
        # `dbutils` only exists inside a Databricks notebook/job runtime.
        return dbutils.secrets.get(scope=scope, key=secret_key)  # type: ignore[name-defined]  # noqa: F821
    except NameError:
        return os.environ.get(env_var_name)


@unique
class WelbeOneEnv(IntEnum):
    """Enumeration of the environments this pipeline can run against."""

    PREPROD = 1
    EDW_LANDING = 2


class EnvConfig:
    """Environment-specific configuration values.

    These values are typically substituted per-environment by a
    DevOps/CI pipeline (e.g. dev / preprod / prod), except for the
    secret values which are always resolved at runtime via
    :func:`_get_secret`.
    """

    # Base URL of the Databricks workspace this job monitors.
    DBW_WS_BASE_URL: str = os.environ.get(
        "DBW_WS_BASE_URL", "https://<your-workspace>.cloud.databricks.com"
    )

    # Databricks Personal Access Token used to call the Jobs API.
    # Resolved from a secret scope / environment variable - never hardcoded.
    DATABRICKS_ADMIN_PAT: Optional[str] = _get_secret(
        secret_key="databricks-pat", env_var_name="DATABRICKS_ADMIN_PAT"
    )


class SMTPConfig:
    """SMTP configuration used for sending email notifications."""

    SMTP_SERVER: str = os.environ.get("SMTP_SERVER", "smtp.office365.com")
    SMTP_PORT: int = int(os.environ.get("SMTP_PORT", "587"))
    SENDER_EMAIL: str = os.environ.get("SENDER_EMAIL", "your-sender@example.com")

    # Resolved from a secret scope / environment variable - never hardcoded.
    SMTP_PASSWORD: Optional[str] = _get_secret(
        secret_key="smtp-password", env_var_name="SMTP_PASSWORD"
    )


class NotificationConfig:
    """Recipient lists and SLA settings for workflow notifications."""

    # Semicolon-separated list of recipients for failure notifications.
    TO_FAILED_EMAIL_LIST: str = os.environ.get(
        "TO_FAILED_EMAIL_LIST", "person1@example.com;person2@example.com"
    )

    # Semicolon-separated list of recipients for success notifications.
    TO_SUCCESS_EMAIL_LIST: str = os.environ.get(
        "TO_SUCCESS_EMAIL_LIST", "person1@example.com;person2@example.com"
    )

    # How many days of job-run history to pull back from the Jobs API.
    DAYS_BACK_IN_HISTORY: int = int(os.environ.get("DAYS_BACK_IN_HISTORY", "1"))

    # SLA cut-off times per data layer, used to flag whether a
    # workflow completed on time.
    SLA_BY_LAYER: dict[str, str] = {
        "Bronze": "4:30 AM EST",
        "Gold": "5:30 AM EST",
    }

    # Unity Catalog tables used to log workflow runs and notifications.
    WORKFLOW_RUN_LOG_TABLE: str = os.environ.get(
        "WORKFLOW_RUN_LOG_TABLE", "workspace.silver.workflow_run_log"
    )
    NOTIFICATION_LOG_TABLE: str = os.environ.get(
        "NOTIFICATION_LOG_TABLE", "workspace.silver.notification_log"
    )
