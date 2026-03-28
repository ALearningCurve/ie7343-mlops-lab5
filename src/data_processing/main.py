from __future__ import annotations

from datetime import datetime, timezone
import io
import json
import os
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
import uuid

from flask import Request
from google.cloud import storage
import pandas as pd


EXPECTED_COLUMNS: List[str] = [
    "sepal_length",
    "sepal_width",
    "petal_length",
    "petal_width",
    "species",
]
NUMERIC_FEATURE_COLUMNS: List[str] = [
    "sepal_length",
    "sepal_width",
    "petal_length",
    "petal_width",
]
MAX_NULL_RATIO: float = 0.10
OUTLIER_Z_THRESHOLD: float = 3.0
MAX_OUTLIER_RATIO: float = 0.05
MEAN_SHIFT_STD_MULTIPLIER: float = 1.0
STD_RATIO_LIMIT: float = 1.5
BASELINE_BLOB: str = "metadata/baseline_stats.json"
RUN_LOG_PREFIX: str = "metadata/runs"

storage_client = storage.Client()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float:
    return float(value) if value is not None else 0.0


def _load_json_blob(bucket: storage.Bucket, blob_name: str) -> Optional[Dict[str, Any]]:
    blob: storage.Blob = bucket.blob(blob_name)
    if not blob.exists():
        return None
    return json.loads(blob.download_as_text())


def _upload_json_blob(
    bucket: storage.Bucket, blob_name: str, payload: Dict[str, Any]
) -> None:
    blob: storage.Blob = bucket.blob(blob_name)
    blob.upload_from_string(
        data=json.dumps(payload),
        content_type="application/json",
    )


def _schema_check(df: pd.DataFrame) -> Dict[str, Any]:
    observed_columns: List[str] = list(df.columns)
    missing: List[str] = [
        column for column in EXPECTED_COLUMNS if column not in observed_columns
    ]
    extras: List[str] = [
        column for column in observed_columns if column not in EXPECTED_COLUMNS
    ]
    passed: bool = not missing and not extras
    return {
        "passed": passed,
        "missing_columns": missing,
        "extra_columns": extras,
    }


def _null_check(df: pd.DataFrame) -> Dict[str, Any]:
    null_ratios: Dict[str, float] = {
        column: _safe_float(df[column].isna().mean()) for column in df.columns
    }
    violating_columns: Dict[str, float] = {
        column: ratio for column, ratio in null_ratios.items() if ratio > MAX_NULL_RATIO
    }
    return {
        "passed": not violating_columns,
        "max_allowed_ratio": MAX_NULL_RATIO,
        "null_ratios": null_ratios,
        "violating_columns": violating_columns,
    }


def _outlier_check(df: pd.DataFrame) -> Dict[str, Any]:
    details: Dict[str, Dict[str, float]] = {}
    failed_columns: List[str] = []

    for column in NUMERIC_FEATURE_COLUMNS:
        if column not in df.columns:
            continue
        series: pd.Series = df[column]
        mean_value: float = _safe_float(series.mean())
        std_value: float = _safe_float(series.std())
        if std_value == 0.0:
            outlier_ratio = 0.0
        else:
            z_scores: pd.Series = (series - mean_value).abs() / std_value
            outlier_ratio = _safe_float((z_scores > OUTLIER_Z_THRESHOLD).mean())
        details[column] = {
            "outlier_ratio": outlier_ratio,
            "max_allowed_ratio": MAX_OUTLIER_RATIO,
        }
        if outlier_ratio > MAX_OUTLIER_RATIO:
            failed_columns.append(column)

    return {
        "passed": not failed_columns,
        "columns": details,
        "failed_columns": failed_columns,
    }


def _compute_current_feature_stats(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    feature_stats: Dict[str, Dict[str, float]] = {}
    for column in NUMERIC_FEATURE_COLUMNS:
        if column not in df.columns:
            continue
        series: pd.Series = df[column]
        feature_stats[column] = {
            "mean": _safe_float(series.mean()),
            "std": _safe_float(series.std()),
            "min": _safe_float(series.min()),
            "max": _safe_float(series.max()),
        }
    return feature_stats


def _drift_check(
    current_stats: Dict[str, Dict[str, float]],
    baseline_stats: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if not baseline_stats:
        return {
            "baseline_available": False,
            "drift_detected": False,
            "passed": True,
            "method": "mean_std_threshold",
            "details": {},
        }

    baseline_features: Dict[str, Dict[str, float]] = baseline_stats.get(
        "feature_stats", {}
    )
    details: Dict[str, Dict[str, Any]] = {}
    drift_detected: bool = False

    for feature_name, current in current_stats.items():
        baseline: Optional[Dict[str, float]] = baseline_features.get(feature_name)
        if not baseline:
            continue

        baseline_mean: float = _safe_float(baseline.get("mean"))
        baseline_std: float = _safe_float(baseline.get("std"))
        current_mean: float = _safe_float(current.get("mean"))
        current_std: float = _safe_float(current.get("std"))

        mean_shift_abs: float = abs(current_mean - baseline_mean)
        if baseline_std > 0:
            mean_shift_limit: float = MEAN_SHIFT_STD_MULTIPLIER * baseline_std
        else:
            mean_shift_limit = MEAN_SHIFT_STD_MULTIPLIER

        std_ratio: float
        if baseline_std > 0:
            std_ratio = current_std / baseline_std
        else:
            std_ratio = 1.0

        mean_violation: bool = mean_shift_abs > mean_shift_limit
        std_violation: bool = std_ratio > STD_RATIO_LIMIT or std_ratio < (
            1 / STD_RATIO_LIMIT
        )
        feature_drifted: bool = mean_violation or std_violation
        if feature_drifted:
            drift_detected = True

        details[feature_name] = {
            "drifted": feature_drifted,
            "mean_shift_abs": mean_shift_abs,
            "mean_shift_limit": mean_shift_limit,
            "std_ratio": std_ratio,
            "std_ratio_limits": [1 / STD_RATIO_LIMIT, STD_RATIO_LIMIT],
            "mean_violation": mean_violation,
            "std_violation": std_violation,
        }

    return {
        "baseline_available": True,
        "drift_detected": drift_detected,
        "passed": not drift_detected,
        "method": "mean_std_threshold",
        "details": details,
    }


def process_data(request: Request) -> tuple[Dict[str, Any], int]:
    """HTTP Cloud Function that validates data quality and computes drift checks."""
    request_json: Optional[Dict[str, Any]] = request.get_json(silent=True)
    if not request_json:
        return {"error": "Request JSON body is required."}, 400

    bucket_name: Optional[str] = request_json.get("bucket") or os.getenv("GCS_BUCKET")
    file_name: Optional[str] = request_json.get("file")
    run_id: str = str(request_json.get("run_id") or uuid.uuid4())

    if not bucket_name or not file_name:
        return {"error": "Both 'bucket' and 'file' are required."}, 400

    bucket: storage.Bucket = storage_client.bucket(bucket_name)
    blob: storage.Blob = bucket.blob(file_name)
    file_contents: str = blob.download_as_text()
    data: pd.DataFrame = pd.read_csv(io.StringIO(file_contents))

    schema_result: Dict[str, Any] = _schema_check(data)
    null_result: Dict[str, Any] = _null_check(data)
    outlier_result: Dict[str, Any] = _outlier_check(data)
    current_feature_stats: Dict[str, Dict[str, float]] = _compute_current_feature_stats(
        data
    )
    baseline_stats: Optional[Dict[str, Any]] = _load_json_blob(bucket, BASELINE_BLOB)
    drift_result: Dict[str, Any] = _drift_check(current_feature_stats, baseline_stats)

    quality_passed: bool = (
        schema_result["passed"]
        and null_result["passed"]
        and outlier_result["passed"]
        and drift_result["passed"]
    )
    action: str = "train" if quality_passed else "skip"

    result: Dict[str, Any] = {
        "status": "data_processed",
        "run_id": run_id,
        "timestamp": _utc_now_iso(),
        "bucket": bucket_name,
        "file": file_name,
        "row_count": int(len(data)),
        "feature_stats": current_feature_stats,
        "quality_gate": {
            "passed": quality_passed,
            "action": action,
            "checks": {
                "schema": schema_result,
                "nulls": null_result,
                "outliers": outlier_result,
                "drift": drift_result,
            },
        },
    }

    _upload_json_blob(bucket, f"{RUN_LOG_PREFIX}/{run_id}.json", result)
    return result, 200
