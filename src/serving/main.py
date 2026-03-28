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
import numpy as np
from numpy.typing import NDArray
import pandas as pd
from sklearn.base import BaseEstimator


MODEL_BLOB_PATH: str = "model.pkl"
MODEL_INFO_BLOB_PATH: str = "metadata/model_info.json"
DEFAULT_BATCH_SIZE: int = 64

storage_client = storage.Client()
model_cache: Optional[BaseEstimator] = None
cached_bucket_name: Optional[str] = None
model_info_cache: Optional[Dict[str, Any]] = None
model_info_bucket_name: Optional[str] = None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_bucket_name(request_json: Optional[Dict[str, Any]]) -> Optional[str]:
    if request_json and request_json.get("bucket"):
        return str(request_json["bucket"])
    return os.getenv("GCS_BUCKET")


def _load_json_blob(bucket: storage.Bucket, blob_name: str) -> Optional[Dict[str, Any]]:
    blob: storage.Blob = bucket.blob(blob_name)
    if not blob.exists():
        return None
    return json.loads(blob.download_as_text())


def _load_model(bucket_name: str) -> BaseEstimator:
    global model_cache
    global cached_bucket_name
    if model_cache is not None and cached_bucket_name == bucket_name:
        return model_cache

    bucket: storage.Bucket = storage_client.bucket(bucket_name)
    model_blob: storage.Blob = bucket.blob(MODEL_BLOB_PATH)
    local_model_path: str = "/tmp/model.pkl"
    model_blob.download_to_filename(local_model_path)
    model_cache = joblib.load(local_model_path)
    cached_bucket_name = bucket_name
    return model_cache


def _load_model_info(bucket_name: str) -> Dict[str, Any]:
    global model_info_cache
    global model_info_bucket_name
    if model_info_cache is not None and model_info_bucket_name == bucket_name:
        return model_info_cache
    bucket: storage.Bucket = storage_client.bucket(bucket_name)
    model_info_cache = _load_json_blob(bucket, MODEL_INFO_BLOB_PATH) or {}
    model_info_bucket_name = bucket_name
    return model_info_cache


def _validate_features(
    request_json: Dict[str, Any], feature_count: Optional[int]
) -> NDArray[np.float64]:
    if "features" not in request_json:
        raise ValueError("Missing 'features' in request body.")
    raw_features: Any = request_json["features"]
    if not isinstance(raw_features, list) or not raw_features:
        raise ValueError("'features' must be a non-empty list.")

    features_array: NDArray[np.float64] = np.asarray(
        raw_features, dtype=np.float64
    ).reshape(1, -1)
    if feature_count is not None and int(features_array.shape[1]) != feature_count:
        raise ValueError(
            f"Feature count mismatch. Expected {feature_count}, got {int(features_array.shape[1])}."
        )
    return features_array


def predict_online(request: Request) -> tuple[Dict[str, Any], int]:
    """HTTP Cloud Function for single-record online prediction."""
    request_json: Optional[Dict[str, Any]] = request.get_json(silent=True)
    if not request_json:
        return {"error": "Request JSON body is required."}, 400

    bucket_name: Optional[str] = _get_bucket_name(request_json)
    if not bucket_name:
        return {
            "error": "Missing 'bucket' in request and GCS_BUCKET env var is not set."
        }, 400

    try:
        model: BaseEstimator = _load_model(bucket_name)
        model_info: Dict[str, Any] = _load_model_info(bucket_name)
        feature_columns: list[str] = model_info.get("feature_columns", [])
        expected_feature_count: Optional[int] = (
            len(feature_columns) if feature_columns else None
        )
        features_array: NDArray[np.float64] = _validate_features(
            request_json, expected_feature_count
        )

        prediction: NDArray[Any] = model.predict(features_array)
        response: Dict[str, Any] = {
            "mode": "online",
            "prediction": prediction.tolist(),
            "run_id": str(request_json.get("run_id") or uuid.uuid4()),
            "model_run_id": model_info.get("run_id"),
            "timestamp": _utc_now_iso(),
        }

        if hasattr(model, "predict_proba"):
            probabilities: NDArray[np.float64] = model.predict_proba(features_array)  # type: ignore[assignment]
            response["confidence"] = float(np.max(probabilities[0]))

        return response, 200
    except ValueError as err:
        return {"error": str(err)}, 400
    except Exception as err:  # noqa: BLE001
        return {"error": f"Prediction failed: {err}"}, 500


def batch_predict(request: Request) -> tuple[Dict[str, Any], int]:
    """HTTP Cloud Function for batch predictions from a CSV file in Cloud Storage."""
    request_json: Optional[Dict[str, Any]] = request.get_json(silent=True)
    if not request_json:
        return {"error": "Request JSON body is required."}, 400

    bucket_name: Optional[str] = _get_bucket_name(request_json)
    input_file: Optional[str] = request_json.get("input_file")
    output_file: str = str(request_json.get("output_file") or "predictions_batch.csv")
    batch_size: int = int(request_json.get("batch_size") or DEFAULT_BATCH_SIZE)
    run_id: str = str(request_json.get("run_id") or uuid.uuid4())

    if not bucket_name:
        return {
            "error": "Missing 'bucket' in request and GCS_BUCKET env var is not set."
        }, 400
    if not input_file:
        return {"error": "Missing required field 'input_file'."}, 400

    try:
        model: BaseEstimator = _load_model(bucket_name)
        model_info: Dict[str, Any] = _load_model_info(bucket_name)
        feature_columns: list[str] = model_info.get("feature_columns", [])

        bucket: storage.Bucket = storage_client.bucket(bucket_name)
        input_blob: storage.Blob = bucket.blob(input_file)
        csv_text: str = input_blob.download_as_text()
        data: pd.DataFrame = pd.read_csv(io.StringIO(csv_text))

        if not feature_columns:
            feature_columns = [
                column
                for column in data.columns
                if column not in {"species", "prediction"}
            ]
        missing_features: list[str] = [
            col for col in feature_columns if col not in data.columns
        ]
        if missing_features:
            return {
                "error": "Input file is missing required feature columns.",
                "missing_features": missing_features,
            }, 400

        predictions: list[Any] = []
        feature_frame: pd.DataFrame = data[feature_columns]
        total_rows: int = int(len(feature_frame))

        for start_index in range(0, total_rows, batch_size):
            end_index: int = min(start_index + batch_size, total_rows)
            batch_frame: pd.DataFrame = feature_frame.iloc[start_index:end_index]
            batch_array: NDArray[np.float64] = np.asarray(batch_frame, dtype=np.float64)
            batch_predictions: NDArray[Any] = model.predict(batch_array)
            predictions.extend(batch_predictions.tolist())

        output_data: pd.DataFrame = data.copy()
        output_data["prediction"] = predictions
        output_data["run_id"] = run_id
        output_data["predicted_at"] = _utc_now_iso()

        local_output_path: str = f"/tmp/{os.path.basename(output_file)}"
        output_data.to_csv(local_output_path, index=False)
        output_blob: storage.Blob = bucket.blob(output_file)
        output_blob.upload_from_filename(local_output_path)

        return {
            "mode": "batch",
            "status": "completed",
            "run_id": run_id,
            "input_file": input_file,
            "output_file": output_file,
            "rows_processed": total_rows,
            "bucket": bucket_name,
            "timestamp": _utc_now_iso(),
        }, 200
    except ValueError as err:
        return {"error": str(err)}, 400
    except Exception as err:  # noqa: BLE001
        return {"error": f"Batch prediction failed: {err}"}, 500


def predict(request: Request) -> tuple[Dict[str, Any], int]:
    """Backward-compatible alias for the original serving entrypoint."""
    return predict_online(request)
