# Databricks notebook source
"""
job_monitoring_main
=====================
Main orchestration notebook for the Job Monitoring solution.

What this notebook does, step by step:
1. Ensures the logging tables (`workflow_run_log`, `notification_log`) exist.
2. Pulls the last N days of Databricks job runs via the Jobs API.
3. Flattens the runs and keeps only today's most-recent run per job.
4. Joins the runs against the declared workflow dependencies
   (see `workflow_dependencies.py`) to see which "layers" are complete.
5. Sends a FAILURE email if any dependency failed and hasn't already
   been notified about today.
6. Sends a SUCCESS email once every dependency for a layer has
   completed and hasn't already been notified about today.
7. Logs every run and every notification sent to Unity Catalog tables
   for audit/history purposes.

This file keeps the `# COMMAND ----------` cell markers so it can be
imported directly as a Databricks notebook, while delegating all
reusable logic to the sibling modules:
`config.py`, `databricks_api_client.py`, `workflow_dependencies.py`,
`job_run_processor.py`, `email_utils.py`, `file_utils.py`.
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.silver.workflow_run_log (
# MAGIC   RunId BIGINT,
# MAGIC   attempt_number INT,
# MAGIC   WorkFlowId BIGINT,
# MAGIC   WorkFlowStartType STRING,
# MAGIC   WorkFlowStartDttm STRING,
# MAGIC   WorkFlowEndDttm STRING,
# MAGIC   WorkFlowDuration STRING,
# MAGIC   WorkFlowState STRING,
# MAGIC   WorkFlowMessage STRING,
# MAGIC   WorkFlowMetSLAFlag STRING,
# MAGIC   InsertedOn TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.silver.notification_log (
# MAGIC   ObjectType STRING,
# MAGIC   WorkflowName STRING,
# MAGIC   job_id STRING,
# MAGIC   EventType STRING,
# MAGIC   EventDTMM TIMESTAMP,
# MAGIC   NotificationDttm TIMESTAMP,
# MAGIC   NotificationSentTo STRING,
# MAGIC   NotificationEmailContent STRING,
# MAGIC   layer STRING,
# MAGIC   InsertedOn TIMESTAMP
# MAGIC )

# COMMAND ----------

from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, current_timestamp, to_date, current_date
from pyspark.sql.window import Window

from config import EnvConfig, SMTPConfig, NotificationConfig
from databricks_api_client import DatabricksAPI
from email_utils import build_status_html_table, send_email_smtp
from job_run_processor import build_runs_df
from workflow_dependencies import build_dependencies_df

# COMMAND ----------

# ---------------------------------------------------------------------------
# Step 1: Setup - client, "today" labels, dependency table
# ---------------------------------------------------------------------------

api_client = DatabricksAPI(base_url=EnvConfig.DBW_WS_BASE_URL, token=EnvConfig.DATABRICKS_ADMIN_PAT)

formatted_date = spark.sql(
    """
    SELECT concat(
        dayofmonth(current_date()),
        CASE
            WHEN dayofmonth(current_date()) IN (11,12,13) THEN 'th'
            WHEN dayofmonth(current_date()) % 10 = 1 THEN 'st'
            WHEN dayofmonth(current_date()) % 10 = 2 THEN 'nd'
            WHEN dayofmonth(current_date()) % 10 = 3 THEN 'rd'
            ELSE 'th'
        END,
        date_format(current_date(), ' MMMM yyyy')
    ) AS today
    """
).first()["today"]

df_dependencies = build_dependencies_df(spark)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Step 2: Pull job run history from the Databricks Jobs API and flatten it
# ---------------------------------------------------------------------------

job_runs = api_client.get_job_runs(days_back=NotificationConfig.DAYS_BACK_IN_HISTORY)
print("Total Job History:", len(job_runs["runs"]))

df_runs = build_runs_df(spark, job_runs["runs"])
print("Total Job History (flattened):", df_runs.count())

# COMMAND ----------

# ---------------------------------------------------------------------------
# Step 3: Upsert every run into the audit log table, keep today's latest
#         run per job only for the comparison logic below.
# ---------------------------------------------------------------------------

df_runs.createOrReplaceTempView("WorkflowHistory")

# MAGIC %sql
# MAGIC MERGE INTO workspace.silver.workflow_run_log w
# MAGIC USING WorkflowHistory l
# MAGIC ON w.RunId = l.run_id
# MAGIC    AND w.WorkFlowId = l.job_id
# MAGIC    AND w.attempt_number = COALESCE(l.attempt_number, 0)
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   w.RunId = l.run_id,
# MAGIC   w.attempt_number = COALESCE(l.attempt_number, 0),
# MAGIC   w.WorkFlowId = l.job_id,
# MAGIC   w.WorkFlowStartType = l.trigger,
# MAGIC   w.WorkFlowStartDttm = l.start_time_in_est,
# MAGIC   w.WorkFlowEndDttm = l.end_time_in_est,
# MAGIC   w.WorkFlowDuration = l.run_duration_min,
# MAGIC   w.WorkFlowState = l.result_state,
# MAGIC   w.WorkFlowMessage = l.state_message,
# MAGIC   w.InsertedOn = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   RunId, attempt_number, WorkFlowId, WorkFlowStartType, WorkFlowStartDttm,
# MAGIC   WorkFlowEndDttm, WorkFlowDuration, WorkFlowState, WorkFlowMessage, InsertedOn
# MAGIC ) VALUES (
# MAGIC   l.run_id, COALESCE(l.attempt_number, 0), l.job_id, l.trigger, l.start_time_in_est,
# MAGIC   l.end_time_in_est, l.run_duration_min, l.result_state, l.state_message, current_timestamp()
# MAGIC )

# COMMAND ----------

window_spec = Window.partitionBy("job_id").orderBy(F.col("end_time_in_est").desc())
df_runs = df_runs.withColumn("rank", F.row_number().over(window_spec))

today_date = date.today().strftime("%Y-%m-%d")
df_runs = df_runs.filter((F.col("start_time_in_est").cast("date") == today_date) & (F.col("rank") == 1))
df_runs = df_runs.withColumn("BronzeWorkFlowName", col("run_name"))

# COMMAND ----------

# ---------------------------------------------------------------------------
# Step 4: Join dependencies against today's runs to determine layer status
# ---------------------------------------------------------------------------

comparison_df = df_dependencies.join(df_runs, df_dependencies["DependsOn"] == df_runs["run_name"], "left_outer")
comparison_df_files = comparison_df.join(
    df_runs, comparison_df["DependsOn"] == df_runs["BronzeWorkFlowName"], "left_outer"
).select(comparison_df["*"])

comparison_df_files = comparison_df_files.withColumn(
    "SLA",
    when(col("layer") == "Bronze", lit(NotificationConfig.SLA_BY_LAYER["Bronze"]))
    .when(col("layer") == "Gold", lit(NotificationConfig.SLA_BY_LAYER["Gold"]))
    .otherwise(None),
)
comparison_df_files = comparison_df_files.withColumn(
    "result_state",
    when((col("state_message") == "In run") & (col("run_name").isNotNull()), lit("SUCCESS")).otherwise(
        col("result_state")
    ),
)
comparison_df_files = comparison_df_files.withColumn("EventType", col("result_state"))

# Exclude anything we've already notified about today.
lookup_df = spark.table(NotificationConfig.NOTIFICATION_LOG_TABLE).filter(to_date(col("InsertedOn")) == current_date())
comparison_df_files = comparison_df_files.join(
    lookup_df.select("job_id", "EventType").distinct(), on=["job_id", "EventType"], how="left_anti"
)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Step 5: Aggregate per data layer (Bronze / Gold / ...)
# ---------------------------------------------------------------------------

comparison_df_count_layer = comparison_df_files.groupBy("Layer", "SLA").agg(
    F.countDistinct("DependsOn").alias("DependsOnWorkflowCount"),
    F.countDistinct("run_name").alias("RunWorkflowCount"),
    F.countDistinct(F.when(F.col("result_state") == "SUCCESS", F.col("run_name"))).alias("SuccessCount"),
    F.countDistinct(F.when(F.col("result_state") != "SUCCESS", F.col("run_name"))).alias("FailCount"),
    F.from_utc_timestamp(
        F.max(F.when(F.col("run_name") != "DE-Bronze-Audit_Smartsheets", F.col("end_time_in_est"))),
        "US/Eastern",
    ).alias("LatestRunTime"),
)

comparison_df_count_layer = (
    comparison_df_count_layer.withColumn(
        "LatestRunTime_only", F.concat(F.date_format(F.col("LatestRunTime"), "h:mm a"), F.lit(" EST"))
    )
    .withColumn(
        "Status",
        F.when(col("DependsOnWorkflowCount") == col("SuccessCount"), lit("Completed")).otherwise(lit("Failed")),
    )
    .withColumn("SLA_ts", F.to_timestamp(F.regexp_replace("SLA", " EST", ""), "h:mm a"))
    .withColumn("LatestRunTime_ts", F.to_timestamp(F.date_format(F.col("LatestRunTime"), "h:mm a"), "h:mm a"))
    .withColumn(
        "SLA_MET",
        F.when(
            (F.hour("LatestRunTime_ts") * 60 + F.minute("LatestRunTime_ts"))
            <= (F.hour("SLA_ts") * 60 + F.minute("SLA_ts")),
            F.lit("Y"),
        ).otherwise(F.lit("N")),
    )
    .withColumn("Comment", F.lit(""))
)

email_df = comparison_df_count_layer.select(
    col("Layer").alias("Data Layer"),
    col("SLA").alias("SLA"),
    col("LatestRunTime_only").alias("Completion Time"),
    col("Status").alias("Status"),
    col("SLA_MET").alias("SLA Met(Y/N)"),
    col("Comment"),
).sort("Layer")

# COMMAND ----------


def _pretty_join_layer_names(layers: list[str]) -> str:
    """Join a list of layer names into a natural-language string.

    e.g. ``["Bronze"]`` -> ``"Bronze"``;
    ``["Bronze", "Gold"]`` -> ``"Bronze and Gold"``;
    ``["A", "B", "C"]`` -> ``"A, B and C"``.
    """
    sorted_layers = sorted(layers, key=str.lower)
    if not sorted_layers:
        return ""
    if len(sorted_layers) == 1:
        return sorted_layers[0]
    if len(sorted_layers) == 2:
        return " and ".join(sorted_layers)
    return ", ".join(sorted_layers[:-1]) + " and " + sorted_layers[-1]


def _log_notifications(df, event_type_col: str, recipients: str, html_body: str) -> None:
    """Append a notification record for every row in `df` to the notification log table."""
    df_to_insert = (
        df.withColumn("ObjectType", lit("WF"))
        .withColumn("WorkflowName", col("run_name"))
        .withColumn("job_id", col("job_id"))
        .withColumn("EventType", col(event_type_col))
        .withColumn("EventDTMM", col("end_time_in_est"))
        .withColumn("NotificationDttm", current_timestamp())
        .withColumn("NotificationSentTo", lit(recipients))
        .withColumn("NotificationEmailContent", lit(html_body))
        .withColumn("layer", col("layer"))
        .withColumn("InsertedOn", current_timestamp())
        .select(
            "ObjectType",
            "WorkflowName",
            "job_id",
            "EventType",
            "EventDTMM",
            "NotificationDttm",
            "NotificationSentTo",
            "NotificationEmailContent",
            "layer",
            "InsertedOn",
        )
        .distinct()
    )
    df_to_insert.write.mode("append").insertInto(NotificationConfig.NOTIFICATION_LOG_TABLE)


# COMMAND ----------

# ---------------------------------------------------------------------------
# Step 6: Failure notification
# ---------------------------------------------------------------------------

failed_wf_df_list = comparison_df_files.filter(col("result_state") == "FAILED")
lookup_df = spark.table(NotificationConfig.NOTIFICATION_LOG_TABLE).filter(to_date(col("InsertedOn")) == current_date())
failed_wf_df_list = failed_wf_df_list.join(lookup_df.select("job_id").distinct(), "job_id", "left_anti")

if failed_wf_df_list.count() > 0:
    failed_wf_df = failed_wf_df_list.select(
        col("run_name").alias("WorkflowName"),
        col("run_id").alias("RunId"),
        "Layer",
        "SLA",
        col("result_state").alias("Status"),
        col("state_message").alias("StatusMessage"),
    ).distinct()

    failed_layers = [row["layer"] for row in failed_wf_df_list.select("layer").distinct().collect()]
    layer_display_str = _pretty_join_layer_names(failed_layers)

    print("Sending failure notification for layers:", layer_display_str)
    send_to = [e.strip() for e in NotificationConfig.TO_FAILED_EMAIL_LIST.split(";")]

    email_subject = f"Prod - EDW (Databricks) - Data refresh - {formatted_date}"
    email_intro = (
        f"Hello All,<br><br>Data refresh for {layer_display_str} layer tables has failed. "
        "Below are the failed refresh timings.<br>"
    )
    html_body = build_status_html_table(email_intro, failed_wf_df.orderBy("SLA"))

    send_email_smtp(
        smtp_server=SMTPConfig.SMTP_SERVER,
        smtp_port=SMTPConfig.SMTP_PORT,
        sender_email=SMTPConfig.SENDER_EMAIL,
        sender_password=SMTPConfig.SMTP_PASSWORD,
        send_to=send_to,
        subject=email_subject,
        html_body=html_body,
    )

    _log_notifications(
        failed_wf_df_list, "result_state", NotificationConfig.TO_FAILED_EMAIL_LIST, html_body
    )

# COMMAND ----------

# ---------------------------------------------------------------------------
# Step 7: Success notification (only once every dependency for every layer
#         has completed)
# ---------------------------------------------------------------------------

total_layers = comparison_df_count_layer.count()
matched_layers = comparison_df_count_layer.filter(col("DependsOnWorkflowCount") == col("SuccessCount")).count()

if total_layers == matched_layers and email_df.count() > 0:
    completed_layers = [row["Layer"] for row in comparison_df_count_layer.select("Layer").distinct().collect()]
    layer_display_str = _pretty_join_layer_names(completed_layers)

    print("Sending success notification for layers:", layer_display_str)
    send_to = [e.strip() for e in NotificationConfig.TO_SUCCESS_EMAIL_LIST.split(";")]

    email_subject = f"Prod - EDW (Databricks) - Data refresh - {formatted_date}"
    email_intro = (
        f"Hello All,<br><br>Data refresh for {layer_display_str} layer tables has completed successfully.<br>"
    )
    html_body = build_status_html_table(email_intro, email_df.orderBy("Layer"))

    send_email_smtp(
        smtp_server=SMTPConfig.SMTP_SERVER,
        smtp_port=SMTPConfig.SMTP_PORT,
        sender_email=SMTPConfig.SENDER_EMAIL,
        sender_password=SMTPConfig.SMTP_PASSWORD,
        send_to=send_to,
        subject=email_subject,
        html_body=html_body,
        send_cc=send_to,
    )

    _log_notifications(
        comparison_df_files, "result_state", NotificationConfig.TO_SUCCESS_EMAIL_LIST, html_body
    )
else:
    print("No email sent - not all dependencies have completed yet.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.notification_log
# MAGIC WHERE CAST(NotificationDttm AS DATE) = current_date()
# MAGIC ORDER BY 1 DESC
