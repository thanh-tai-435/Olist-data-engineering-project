"""
Delivery Delay Prediction — XGBoost Regressor
Reads:   Silver (stg_orders + stg_order_items + stg_products
                 + stg_sellers + stg_customers + stg_order_payments)
Predicts: delivery_delay_days (negative = early, positive = late)
Logs to: MLflow experiment "delivery-delay-prediction"

Usage:
  python ml/training/train_delivery_model.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import logging

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from features import build_delivery_features
from sklearn.compose import ColumnTransformer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder
from utils import get_catalog, setup_mlflow
from xgboost import XGBRegressor

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

EXPERIMENT = "delivery-delay-prediction"
MODEL_NAME = "delivery-delay-xgb"

CAT_COLS = ["customer_state", "seller_state", "payment_type"]
NUM_COLS = [
    "item_count",
    "total_weight_g",
    "order_revenue",
    "order_freight",
    "max_installments",
    "order_month_num",
    "seller_customer_same_state",   # new: shipping distance proxy
    "avg_product_volume_cm3",       # new: bulkiness signal
    "n_product_categories",         # new: order diversity
]
TARGET = "delivery_delay_days"

PARAMS = {
    "n_estimators":     200,
    "max_depth":        6,
    "learning_rate":    0.05,
    "subsample":        0.8,
    "colsample_bytree": 0.8,
    "objective":        "reg:squarederror",
    "random_state":     42,
    "n_jobs":           -1,
}


def train() -> str:
    setup_mlflow()
    catalog = get_catalog()
    df = build_delivery_features(catalog)

    X = df[CAT_COLS + NUM_COLS]
    y = df[TARGET].astype(float)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    preprocessor = ColumnTransformer([
        ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CAT_COLS),
        ("num", "passthrough", NUM_COLS),
    ])

    pipeline = Pipeline([
        ("prep",  preprocessor),
        ("model", XGBRegressor(**PARAMS)),
    ])

    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run() as run:
        mlflow.log_params({
            **PARAMS,
            "train_rows": len(X_train),
            "test_rows":  len(X_test),
            "source":     "silver",
        })
        mlflow.log_param("features", CAT_COLS + NUM_COLS)

        pipeline.fit(X_train, y_train)
        y_pred = pipeline.predict(X_test)

        rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
        mae  = float(mean_absolute_error(y_test, y_pred))
        r2   = float(r2_score(y_test, y_pred))
        mlflow.log_metrics({"rmse": rmse, "mae": mae, "r2": r2})

        xgb = pipeline.named_steps["model"]
        fi  = pd.Series(xgb.feature_importances_,
                        index=CAT_COLS + NUM_COLS).sort_values(ascending=False)
        fi_path = "/tmp/delivery_feature_importance.csv"
        fi.to_csv(fi_path, header=["importance"])
        mlflow.log_artifact(fi_path)
        log.info("Feature importance:\n%s", fi.to_string())

        mlflow.sklearn.log_model(
            pipeline, "model",
            registered_model_name=MODEL_NAME,
            input_example=X_test.head(5),
        )
        log.info("Run %s  RMSE=%.3f  MAE=%.3f  R2=%.3f",
                 run.info.run_id, rmse, mae, r2)
        return run.info.run_id


if __name__ == "__main__":
    train()
