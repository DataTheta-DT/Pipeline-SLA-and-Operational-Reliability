"""
file_utils.py
==============
Small, dependency-light utility functions for:

* walking DBFS/Volumes directory trees (:func:`list_files_recursive`)
* deleting all files in a folder (:func:`remove_files_in_folder`)
* safely traversing nested dict/JSON structures via a path string
  (:func:`json_parse`) - used to pull specific fields out of the
  (deeply nested) Databricks Jobs API run objects.
"""

from __future__ import annotations

from typing import Any, Callable, Optional


def json_parse(raw_json: dict, path: str, type_cast: Optional[Callable] = None) -> Any:
    """Safely traverse a nested dict using a ``"/"``-separated path.

    Equivalent to chained ``.get()`` calls, but tolerant of missing
    intermediate keys (returns ``None`` instead of raising).

    Args:
        raw_json: The dictionary to traverse (e.g. a single "run"
            object returned by the Databricks Jobs API).
        path: A ``/``-separated path of keys, e.g.
            ``"cluster_spec/new_cluster/cluster_name"``.
        type_cast: Optional callable applied to the resolved value
            before returning (e.g. ``int``, ``str``).

    Returns:
        The resolved value (optionally cast), or ``None`` if any part
        of the path is missing or the resolved value is an empty dict.

    Example:
        >>> json_parse({"state": {"result_state": "SUCCESS"}}, "state/result_state")
        'SUCCESS'
    """
    result = raw_json
    for key in path.split("/"):
        if not key:
            continue
        result = result.get(key, {}) if result is not None else {}

    if result is None:
        return None
    if isinstance(result, dict) and len(result) == 0:
        return None
    return type_cast(result) if type_cast else result


def list_files_recursive(dbutils, path: str) -> list[dict]:
    """Recursively list every file and folder under a DBFS/Volumes path.

    Args:
        dbutils: The Databricks ``dbutils`` object (must be passed in
            explicitly since it is only available inside a Databricks
            notebook/job runtime).
        path: The root DBFS or Unity Catalog Volume path to walk.

    Returns:
        A flat list of dicts, one per file/folder found, each with
        ``path``, ``name``, ``modificationTime``, and ``type`` keys.
    """
    files: list[dict] = []
    contents = dbutils.fs.ls(path)

    for item in contents:
        item_type = "folder" if item.isDir() else "file"
        files.append(
            {
                "path": item.path.rstrip("/"),
                "name": item.name.rstrip("/"),
                "modificationTime": item.modificationTime,
                "type": item_type,
            }
        )
        if item.isDir():
            files.extend(list_files_recursive(dbutils, item.path))

    return files


def remove_files_in_folder(dbutils, folder_path: str) -> None:
    """Delete every file/folder directly inside ``folder_path``.

    Args:
        dbutils: The Databricks ``dbutils`` object.
        folder_path: The DBFS/Volumes folder whose contents should be
            removed.
    """
    for file in dbutils.fs.ls(folder_path):
        dbutils.fs.rm(file.path, recurse=True)
