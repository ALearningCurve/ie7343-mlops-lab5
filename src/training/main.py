from __future__ import annotations

from datetime import datetime, timezone
import io
import json
import os
from typing import Any
from typing import Dict
from typing import Optional
import uuid

from flask import Request
from google.cloud import storage
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split


TARGET_COLUMN: str = "species"
MODEL_BLOB_PATH: str = "model.pkl"
MODEL_INFO_BLOB_PATH: str = "metadata/model_info.json"
BASELINE_BLOB_PATH: str = "metadata/baseline_stats.json"
TRAINING_LOG_PREFIX: str = "metadata/training_runs"

storage_client = storage.Client()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float:
    return float(value) if value is not None else 0.0


def _upload_json_blob(
    bucket: storage.Bucket, blob_name: str, payload: Dict[str, Any]
) -> None:
    blob: storage.Blob = bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(payload), content_type="application/json")


def _load_json_blob(bucket: storage.Bucket, blob_name: str) -> Optional[Dict[str, Any]]:
    blob: storage.Blob = bucket.blob(blob_name)
    if not blob.exists():
        return None
    return json.loads(blob.download_as_text())


def _build_baseline_stats(data: pd.DataFrame, run_id: str) -> Dict[str, Any]:
    feature_columns: list[str] = [
        column for column in data.columns if column != TARGET_COLUMN
    ]
    feature_stats: Dict[str, Dict[str, float]] = {}

    for column in feature_columns:
        series: pd.Series = data[column]
        feature_stats[column] = {
            "mean": _safe_float(series.mean()),
            "std": _safe_float(series.std()),
            "min": _safe_float(series.min()),
            "max": _safe_float(series.max()),
        }

    target_distribution: Dict[str, int] = {}
    if TARGET_COLUMN in data.columns:
        distribution_series: pd.Series = data[TARGET_COLUMN].value_counts()
        target_distribution = {
            str(label): int(count) for label, count in distribution_series.items()
        }

    return {
        "run_id": run_id,
        "created_at": _utc_now_iso(),
        "sample_count": int(len(data)),
        "feature_columns": feature_columns,
        "target_column": TARGET_COLUMN,
        "target_distribution": target_distribution,
        "feature_stats": feature_stats,
    }


def train_model(request: Request) -> tuple[Dict[str, Any], int]:
    """HTTP Cloud Function that trains a model only when quality gates pass."""
    request_json: Optional[Dict[str, Any]] = request.get_json(silent=True)
    if not request_json:
        return {"error": "Request JSON body is required."}, 400

    bucket_name: Optional[str] = request_json.get("bucket") or os.getenv("GCS_BUCKET")
    file_name: Optional[str] = request_json.get("file")
    run_id: str = str(request_json.get("run_id") or uuid.uuid4())
    quality_gate: Dict[str, Any] = request_json.get("quality_gate", {})

    if not bucket_name or not file_name:
        return {"error": "Both 'bucket' and 'file' are required."}, 400

    if not bool(quality_gate.get("passed", False)):
        skipped_payload: Dict[str, Any] = {
            "status": "skipped",
            "reason": "quality_gate_failed",
            "run_id": run_id,
            "timestamp": _utc_now_iso(),
        }
        bucket_for_log: storage.Bucket = storage_client.bucket(bucket_name)
        _upload_json_blob(
            bucket_for_log, f"{TRAINING_LOG_PREFIX}/{run_id}.json", skipped_payload
        )
        return skipped_payload, 200

    bucket: storage.Bucket = storage_client.bucket(bucket_name)
    data_blob: storage.Blob = bucket.blob(file_name)
    file_contents: str = data_blob.download_as_text()
    data: pd.DataFrame = pd.read_csv(io.StringIO(file_contents))

    if TARGET_COLUMN not in data.columns:
        return {
            "error": f"Missing target column '{TARGET_COLUMN}' in training data."
        }, 400

    feature_columns: list[str] = [
        column for column in data.columns if column != TARGET_COLUMN
    ]
    X: pd.DataFrame = data[feature_columns]
    y: pd.Series = data[TARGET_COLUMN]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    model: RandomForestClassifier = RandomForestClassifier(
        n_estimators=150, random_state=42
    )
    model.fit(X_train, y_train)
    accuracy: float = _safe_float(model.score(X_test, y_test))

    model_path: str = "/tmp/model.pkl"
    joblib.dump(model, model_path)

    model_blob: storage.Blob = bucket.blob(MODEL_BLOB_PATH)
    model_blob.upload_from_filename(model_path)

    model_info: Dict[str, Any] = {
        "run_id": run_id,
        "trained_at": _utc_now_iso(),
        "model_type": "RandomForestClassifier",
        "feature_columns": feature_columns,
        "target_column": TARGET_COLUMN,
        "metrics": {
            "accuracy": accuracy,
            "train_rows": int(len(X_train)),
            "test_rows": int(len(X_test)),
        },
        "model_blob": MODEL_BLOB_PATH,
    }
    _upload_json_blob(bucket, MODEL_INFO_BLOB_PATH, model_info)

    baseline_exists: bool = _load_json_blob(bucket, BASELINE_BLOB_PATH) is not None
    baseline_written: bool = False
    if not baseline_exists:
        baseline_payload: Dict[str, Any] = _build_baseline_stats(data, run_id)
        _upload_json_blob(bucket, BASELINE_BLOB_PATH, baseline_payload)
        baseline_written = True

    training_result: Dict[str, Any] = {
        "status": "trained",
        "run_id": run_id,
        "timestamp": _utc_now_iso(),
        "model": model_info,
        "baseline_written": baseline_written,
    }
    _upload_json_blob(bucket, f"{TRAINING_LOG_PREFIX}/{run_id}.json", training_result)
    return training_result, 200
