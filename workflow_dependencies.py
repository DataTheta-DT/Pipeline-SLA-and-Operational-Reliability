"""
workflow_dependencies.py
=========================
Declarative configuration describing which Databricks workflows each
"Subject Area" / data layer depends on. This is the source of truth
that :mod:`job_monitoring_main` compares actual job runs against to
determine whether a data layer has completed its refresh for the day.

To onboard a new workflow, add an entry to :data:`WORKFLOW_DEPENDENCIES`
below - no other code changes are required.
"""

from __future__ import annotations

from typing import TypedDict


class DependencyEntry(TypedDict):
    """One row describing a workflow dependency."""

    SubjectArea: str
    WorkflowName: str
    DependsOn: str
    layer: str


# Add / edit workflow dependency rows here.
WORKFLOW_DEPENDENCIES: list[DependencyEntry] = [
    {
        "SubjectArea": "DataModel 1",
        "WorkflowName": "DE-Gold-Workflow-DataModel-1",
        "DependsOn": "Load_edw_Bronze",
        "layer": "Bronze",
    },
    {
        "SubjectArea": "DataModel 1",
        "WorkflowName": "DE-Gold-Workflow-DataModel-1",
        "DependsOn": "Load_edw_Gold",
        "layer": "Bronze",
    },
    {
        "SubjectArea": "DataModel 1",
        "WorkflowName": "DE-Gold-Workflow-DataModel-1",
        "DependsOn": "Load_edw_Silver",
        "layer": "Bronze",
    },
    {
        "SubjectArea": "DataModel 2",
        "WorkflowName": "DE-Gold-Workflow-DataModel-2",
        "DependsOn": "Load_edw_Bronze",
        "layer": "Gold",
    },
    {
        "SubjectArea": "DataModel 2",
        "WorkflowName": "DE-Gold-Workflow-DataModel-2",
        "DependsOn": "Load_edw_Gold",
        "layer": "Gold",
    },
    {
        "SubjectArea": "DataModel 2",
        "WorkflowName": "DE-Gold-Workflow-DataModel-2",
        "DependsOn": "Load_edw_Silver",
        "layer": "Gold",
    },
]


def build_dependencies_df(spark):
    """Build a Spark DataFrame from :data:`WORKFLOW_DEPENDENCIES`.

    Args:
        spark: An active ``SparkSession`` (the notebook-global
            ``spark`` object when run inside Databricks).

    Returns:
        A Spark DataFrame with columns
        ``SubjectArea, WorkflowName, DependsOn, layer``.
    """
    df = spark.createDataFrame(WORKFLOW_DEPENDENCIES)
    return df.select("SubjectArea", "WorkflowName", "DependsOn", "layer")
