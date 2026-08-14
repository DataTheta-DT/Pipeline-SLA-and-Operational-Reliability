"""
job_run_processor.py
======================
Transforms the raw, deeply-nested JSON payload returned by the
Databricks Jobs "runs/list" API into a flat, typed Spark DataFrame
that the rest of the pipeline can filter/join/aggregate easily.
"""

from __future__ import annotations

import arrow

from file_utils import json_parse

# Column name -> Spark SQL type string, used to build the DataFrame schema.
RUN_SCHEMA: dict[str, str] = {
    "run_id": "string",
    "job_id": "string",
    "run_name": "string",
    "start_time_in_ms": "long",
    "start_time_in_pst": "timestamp",
    "start_time_in_est": "timestamp",
    "setup_duration_min": "float",
    "run_duration_min": "float",
    "end_time_in_ms": "long",
    "end_time_in_pst": "timestamp",
    "end_time_in_est": "timestamp",
    "run_page_url": "string",
    "trigger": "string",
    "attempt_number": "integer",
    "result_state": "string",
    "state_message": "string",
    "user_cancelled_or_timedout": "string",
    "cluster_id": "string",
    "cluster_name": "string",
    "spark_version": "string",
    "instance_pool_id": "string",
    "node_type_id": "string",
    "data_security_mode": "string",
    "num_workers": "string",
    "min_workers": "string",
    "max_workers": "string",
    "format": "string",
}


def run_schema_as_ddl() -> str:
    """Render :data:`RUN_SCHEMA` as a Spark DDL schema string.

    Returns:
        A comma-separated ``"col type"`` string usable with
        ``spark.createDataFrame(data, schema=...)``.
    """
    return ", ".join(f"{name} {sql_type}" for name, sql_type in RUN_SCHEMA.items())


def _reshape_single_run(run: dict) -> dict:
    """Flatten one raw Jobs-API run object into a row matching :data:`RUN_SCHEMA`.

    Args:
        run: A single run object as returned inside the ``"runs"``
            list of the Jobs API ``runs/list`` response.

    Returns:
        A flat dict with keys matching :data:`RUN_SCHEMA`.
    """
    cluster_name = json_parse(run, "cluster_spec/new_cluster/cluster_name")
    start_time = json_parse(run, "start_time")
    end_time = json_parse(run, "end_time")
    setup_duration = json_parse(run, "setup_duration")
    run_duration = json_parse(run, "run_duration")

    return {
        "run_id": json_parse(run, "run_id"),
        "job_id": json_parse(run, "job_id"),
        "attempt_number": json_parse(run, "attempt_number"),
        "run_name": json_parse(run, "run_name"),
        "start_time_in_ms": start_time,
        "start_time_in_pst": arrow.get(start_time).to("US/Pacific").naive if start_time else None,
        "start_time_in_est": arrow.get(start_time).to("US/Eastern").naive if start_time else None,
        "setup_duration_min": round(setup_duration / (1000 * 60), 1) if setup_duration else None,
        "run_duration_min": round(run_duration / (1000 * 60), 1) if run_duration else None,
        "end_time_in_ms": end_time,
        "end_time_in_pst": arrow.get(end_time).to("US/Pacific").naive if end_time else None,
        "end_time_in_est": arrow.get(end_time).to("US/Eastern").naive if end_time else None,
        "run_page_url": json_parse(run, "run_page_url"),
        "trigger": json_parse(run, "trigger"),
        "result_state": json_parse(run, "state/result_state"),
        "state_message": json_parse(run, "state/state_message"),
        "user_cancelled_or_timedout": json_parse(run, "state/user_cancelled_or_timedout"),
        "cluster_id": json_parse(run, "cluster_instance/cluster_id"),
        "cluster_name": "CUSTOM" if cluster_name in (None, "") else cluster_name,
        "spark_version": json_parse(run, "cluster_spec/new_cluster/spark_version"),
        "instance_pool_id": json_parse(run, "cluster_spec/new_cluster/instance_pool_id"),
        "node_type_id": json_parse(run, "cluster_spec/new_cluster/node_type_id"),
        "data_security_mode": json_parse(run, "cluster_spec/new_cluster/data_security_mode"),
        "num_workers": json_parse(run, "cluster_spec/new_cluster/num_workers"),
        "min_workers": json_parse(run, "cluster_spec/new_cluster/autoscale/min_workers"),
        "max_workers": json_parse(run, "cluster_spec/new_cluster/autoscale/max_workers"),
        "format": json_parse(run, "format"),
    }


def reshape_runs(raw_runs: list[dict]) -> list[dict]:
    """Flatten a list of raw Jobs-API run objects.

    Args:
        raw_runs: The ``job_runs["runs"]`` list from
            :meth:`databricks_api_client.DatabricksAPI.get_job_runs`.

    Returns:
        A list of flat dicts, one per run, matching :data:`RUN_SCHEMA`.
    """
    return [_reshape_single_run(run) for run in raw_runs]


def build_runs_df(spark, raw_runs: list[dict]):
    """Build the flattened job-runs Spark DataFrame, sorted newest-first.

    Args:
        spark: An active ``SparkSession``.
        raw_runs: The ``job_runs["runs"]`` list from the Jobs API.

    Returns:
        A Spark DataFrame with schema :data:`RUN_SCHEMA`, ordered by
        ``start_time_in_pst`` descending.
    """
    from pyspark.sql.functions import col  # local import: keeps module usable w/o pyspark for unit tests

    reshaped = reshape_runs(raw_runs)
    df = spark.createDataFrame(reshaped, run_schema_as_ddl())
    return df.orderBy(col("start_time_in_pst").desc())
