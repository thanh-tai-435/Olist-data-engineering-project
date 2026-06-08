"""
Delivery Delay Prediction — XGBoost Regression
Reads:   olist.gold.fct_orders
Predicts: delivery_delay_days (negative = early, positive = late)
Logs to: MLflow experiment "delivery-delay-prediction"

Usage:
  python ml/training/train_delivery_model.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import logging
import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from xgboost import XGBRegressor

from utils import load_gold_table, setup_mlflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

EXPERIMENT  = "delivery-delay-prediction"
MODEL_NAME  = "delivery-delay-xgb"

CAT_COLS = ["customer_state", "payment_type"]
NUM_COLS = ["item_count", "total_weight_g", "order_freight",
            "order_revenue", "max_installments", "order_month_num"]
TARGET   = "delivery_delay_days"

PARAMS = {
    "n_estimators":    200,
    "max_depth":       6,
    "learning_rate":   0.05,
    "subsample":       0.8,
    "colsample_bytree": 0.8,
    "objective":       "reg:squarederror",
    "random_state":    42,
    "n_jobs":          -1,
}


def prepare(df: pd.DataFrame):
    df = df[
        (df["order_status"] == "delivered") &
        df[TARGET].notna()
    ].copy()

    df["order_month_num"] = (
        df["order_month"].str.split("-").str[1].astype(float).fillna(1)
    )
    df["total_weight_g"]  = pd.to_numeric(df["total_weight_g"],  errors="coerce").fillna(0)
    df["max_installments"] = pd.to_numeric(df["max_installments"], errors="coerce").fillna(1)
    df[CAT_COLS] = df[CAT_COLS].fillna("unknown").astype(str)

    X = df[CAT_COLS + NUM_COLS]
    y = df[TARGET].astype(float)
    log.info("Training set: %d rows (delivered orders with valid delay)", len(X))
    return X, y


def train() -> str:
    setup_mlflow()
    df = load_gold_table("olist.gold.fct_orders")
    X, y = prepare(df)

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
        mlflow.log_params({**PARAMS, "train_rows": len(X_train), "test_rows": len(X_test)})
        mlflow.log_param("features", CAT_COLS + NUM_COLS)

        pipeline.fit(X_train, y_train)
        y_pred = pipeline.predict(X_test)

        rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
        mae  = float(mean_absolute_error(y_test, y_pred))
        r2   = float(r2_score(y_test, y_pred))
        mlflow.log_metrics({"rmse": rmse, "mae": mae, "r2": r2})

        # Feature importance from inner XGBRegressor
        xgb_model  = pipeline.named_steps["model"]
        fi = pd.Series(
            xgb_model.feature_importances_,
            index=CAT_COLS + NUM_COLS,
        ).sort_values(ascending=False)
        fi_path = "/tmp/delivery_feature_importance.csv"
        fi.to_csv(fi_path, header=["importance"])
        mlflow.log_artifact(fi_path)

        mlflow.sklearn.log_model(
            pipeline, "model",
            registered_model_name=MODEL_NAME,
            input_example=X_test.head(5),
        )

        log.info("Run %s  RMSE=%.3f  MAE=%.3f  R2=%.3f", run.info.run_id, rmse, mae, r2)
        return run.info.run_id


if __name__ == "__main__":
    train()
