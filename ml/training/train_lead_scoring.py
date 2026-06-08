"""
Lead Scoring — XGBoost Binary Classifier
Reads:   olist.gold.fct_funnel
Predicts: is_converted (1 = lead became a closed deal)
Logs to: MLflow experiment "lead-scoring"

Usage:
  python ml/training/train_lead_scoring.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import logging
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score, f1_score, precision_score, recall_score,
    average_precision_score, classification_report
)
from xgboost import XGBClassifier

from utils import load_gold_table, setup_mlflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

EXPERIMENT = "lead-scoring"
MODEL_NAME = "lead-scoring-xgb"

CAT_COLS = ["origin", "business_segment", "lead_type", "business_type"]
NUM_COLS = ["declared_monthly_revenue", "first_contact_month"]
TARGET   = "is_converted"

PARAMS = {
    "n_estimators":    100,
    "max_depth":       4,
    "learning_rate":   0.1,
    "subsample":       0.8,
    "colsample_bytree": 0.8,
    "scale_pos_weight": 8,   # leads dataset heavily imbalanced (~10% conversion)
    "objective":       "binary:logistic",
    "eval_metric":     "aucpr",  # area under precision-recall curve
    "random_state":    42,
    "n_jobs":          -1,
}


def prepare(df: pd.DataFrame):
    df = df.copy()

    # Extract month from first_contact_date as seasonality feature
    df["first_contact_month"] = (
        pd.to_datetime(df["first_contact_date"], errors="coerce").dt.month.fillna(1)
    )
    df["declared_monthly_revenue"] = (
        pd.to_numeric(df["declared_monthly_revenue"], errors="coerce").fillna(0)
    )
    df[CAT_COLS] = df[CAT_COLS].fillna("unknown").astype(str)
    df[TARGET]   = df[TARGET].fillna(0).astype(int)

    X = df[CAT_COLS + NUM_COLS]
    y = df[TARGET]
    conversion_rate = y.mean()
    log.info("Dataset: %d leads  conversion_rate=%.1f%%", len(X), conversion_rate * 100)
    return X, y


def train() -> str:
    setup_mlflow()
    df = load_gold_table("olist.gold.fct_funnel")
    X, y = prepare(df)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    preprocessor = ColumnTransformer([
        ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CAT_COLS),
        ("num", "passthrough", NUM_COLS),
    ])
    pipeline = Pipeline([
        ("prep",  preprocessor),
        ("model", XGBClassifier(**PARAMS)),
    ])

    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run() as run:
        mlflow.log_params({**PARAMS, "train_rows": len(X_train), "test_rows": len(X_test)})
        mlflow.log_param("features", CAT_COLS + NUM_COLS)

        pipeline.fit(X_train, y_train)
        y_pred  = pipeline.predict(X_test)
        y_proba = pipeline.predict_proba(X_test)[:, 1]

        auc     = float(roc_auc_score(y_test, y_proba))
        ap      = float(average_precision_score(y_test, y_proba))  # PR-AUC
        f1      = float(f1_score(y_test, y_pred, zero_division=0))
        prec    = float(precision_score(y_test, y_pred, zero_division=0))
        rec     = float(recall_score(y_test, y_pred, zero_division=0))
        mlflow.log_metrics({"auc": auc, "pr_auc": ap, "f1": f1, "precision": prec, "recall": rec})

        report = classification_report(y_test, y_pred, target_names=["not_converted", "converted"])
        report_path = "/tmp/lead_classification_report.txt"
        with open(report_path, "w") as f:
            f.write(report)
        mlflow.log_artifact(report_path)

        xgb_model = pipeline.named_steps["model"]
        fi = pd.Series(
            xgb_model.feature_importances_,
            index=CAT_COLS + NUM_COLS,
        ).sort_values(ascending=False)
        fi_path = "/tmp/lead_feature_importance.csv"
        fi.to_csv(fi_path, header=["importance"])
        mlflow.log_artifact(fi_path)

        mlflow.sklearn.log_model(
            pipeline, "model",
            registered_model_name=MODEL_NAME,
            input_example=X_test.head(5),
        )

        log.info("Run %s  AUC=%.3f  PR-AUC=%.3f  F1=%.3f", run.info.run_id, auc, ap, f1)
        return run.info.run_id


if __name__ == "__main__":
    train()
