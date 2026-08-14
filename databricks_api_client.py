"""
databricks_api_client.py
=========================
Thin client around the Databricks Jobs REST API (v2.0 / v2.1).

This module isolates all direct HTTP calls to the Databricks
workspace so the rest of the codebase never has to know about
endpoints, headers, or auth. It is used to:

* fetch metadata for a specific job
* trigger ("run now") a job
* list job runs within a lookback window

Example
-------
    from config import EnvConfig
    from databricks_api_client import DatabricksAPI

    client = DatabricksAPI(
        base_url=EnvConfig.DBW_WS_BASE_URL,
        token=EnvConfig.DATABRICKS_ADMIN_PAT,
    )
    runs = client.get_job_runs(days_back=1)
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Optional

import requests


class DatabricksAPI:
    """Client for interacting with the Databricks Jobs REST API.

    Attributes:
        base_url: Base URL of the Databricks workspace, e.g.
            ``https://<workspace>.cloud.databricks.com``.
        token: Personal Access Token (PAT) used for Bearer auth.
    """

    def __init__(self, base_url: str, token: Optional[str]):
        if not base_url:
            raise ValueError("A Databricks workspace base_url is required.")
        if not token:
            raise ValueError(
                "A Databricks PAT is required. Configure it via a secret "
                "scope or the DATABRICKS_ADMIN_PAT environment variable."
            )
        self.base_url = base_url.rstrip("/")
        self.token = token

    def _headers(self) -> dict:
        """Build the standard auth + content-type headers for every request."""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def get_job_details(self, job_id: str) -> dict:
        """Fetch metadata for a single job.

        Uses Jobs API v2.1 (``GET /api/2.1/jobs/get``).

        Args:
            job_id: The numeric Databricks job ID, as a string.

        Returns:
            The parsed JSON response describing the job.

        Raises:
            requests.HTTPError: If the API call does not return a 2xx status.
        """
        url = f"{self.base_url}/api/2.1/jobs/get"
        params = {"job_id": job_id}
        resp = requests.get(url, params=params, headers=self._headers())
        resp.raise_for_status()
        return resp.json()

    def trigger_workflow(self, job_id: str) -> dict:
        """Trigger an immediate ("run now") execution of a job.

        Uses Jobs API v2.1 (``POST /api/2.1/jobs/run-now``).

        Args:
            job_id: The numeric Databricks job ID, as a string.

        Returns:
            The parsed JSON response containing the new ``run_id``.

        Raises:
            requests.HTTPError: If the API call does not return a 2xx status.
        """
        url = f"{self.base_url}/api/2.1/jobs/run-now"
        params = {"job_id": job_id}
        resp = requests.post(url, params=params, headers=self._headers())
        resp.raise_for_status()
        return resp.json()

    def get_job_runs(self, days_back: int) -> dict:
        """List all job runs that started within the given lookback window.

        Uses Jobs API v2.0 (``GET /api/2.0/jobs/runs/list``).

        Args:
            days_back: Number of days to look back from now (UTC) for
                run start times.

        Returns:
            The parsed JSON response, containing a ``"runs"`` key with
            a list of run objects.

        Raises:
            requests.HTTPError: If the API call does not return a 2xx status.
        """
        start_time = datetime.utcnow() - timedelta(days=days_back)
        params = {
            "limit": 1000,
            "start_time_from": int(start_time.timestamp() * 1000),
        }
        url = f"{self.base_url}/api/2.0/jobs/runs/list"
        resp = requests.get(url, headers=self._headers(), params=params)
        resp.raise_for_status()
        return json.loads(resp.content)
