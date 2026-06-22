"""
ML Model Serving API — FastAPI
Loads 3 XGBoost models from MLflow Model Registry and exposes REST endpoints.

Models (trained from Silver layer via feature pipelines):
  delivery-delay-xgb → POST /predict/delivery-delay  (regression, days)
  customer-churn-xgb → POST /predict/churn            (classifier, probability)
  lead-scoring-xgb   → POST /predict/lead             (classifier, probability)

Internal URL:  http://ml-serving:8080
External URL:  via Cloudflare Tunnel (see tunnel-urls.txt)
API docs:      http://localhost:8080/docs
"""
import logging
import os
from contextlib import asynccontextmanager

import mlflow
import mlflow.sklearn
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")

REGISTRY = {
    "delivery": "delivery-delay-xgb",
    "churn":    "customer-churn-xgb",
    "lead":     "lead-scoring-xgb",
}

# Feature column order must match training scripts exactly
DELIVERY_COLS = [
    "customer_state", "seller_state", "payment_type",
    "item_count", "total_weight_g", "order_revenue", "order_freight",
    "max_installments", "order_month_num",
    "seller_customer_same_state", "avg_product_volume_cm3", "n_product_categories",
]
CHURN_COLS = [
    "customer_state",
    "total_orders", "total_spend", "avg_order_value", "is_repeat_customer",
    "orders_last_30d", "orders_last_90d", "orders_last_180d",
    "avg_days_between_orders", "days_since_last_order",
]
LEAD_COLS = ["origin", "first_contact_month", "first_contact_dayofweek"]

models: dict = {}


def load_model(name: str):
    uri = f"models:/{name}/latest"
    log.info("Loading %s from %s ...", name, uri)
    return mlflow.sklearn.load_model(uri)


@asynccontextmanager
async def lifespan(app: FastAPI):
    mlflow.set_tracking_uri(MLFLOW_URI)
    log.info("MLflow URI: %s", MLFLOW_URI)
    for key, name in REGISTRY.items():
        try:
            models[key] = load_model(name)
            log.info("  ✓ %s loaded", name)
        except Exception as exc:
            log.error("  ✗ %s failed: %s", name, exc)
    yield
    models.clear()


app = FastAPI(
    title="Olist ML Serving API",
    version="2.0.0",
    description="XGBoost models trained from Silver Iceberg layer: delivery delay · customer churn · lead scoring",
    lifespan=lifespan,
)


# ── Request / Response schemas ────────────────────────────────────────────────

class DeliveryRequest(BaseModel):
    customer_state:        str   = Field(...,  example="SP")
    seller_state:          str   = Field(...,  example="MG")
    payment_type:          str   = Field(...,  example="credit_card")
    item_count:            float = Field(...,  example=2)
    total_weight_g:        float = Field(...,  example=1500.0)
    order_revenue:         float = Field(...,  example=89.9)
    order_freight:         float = Field(...,  example=18.5)
    max_installments:      float = Field(1.0,  example=3)
    order_month_num:       float = Field(1.0,  example=8)
    avg_product_volume_cm3: float = Field(0.0, example=500.0)
    n_product_categories:  float = Field(1.0,  example=1)
    # seller_customer_same_state is derived server-side from the two states above

class DeliveryResponse(BaseModel):
    delay_days:     float
    interpretation: str

class ChurnRequest(BaseModel):
    customer_state:          str   = Field(...,  example="RJ")
    total_orders:            float = Field(...,  example=3)
    total_spend:             float = Field(...,  example=450.0)
    avg_order_value:         float = Field(...,  example=150.0)
    is_repeat_customer:      float = Field(0.0,  example=1)
    orders_last_30d:         float = Field(0.0,  example=0)
    orders_last_90d:         float = Field(0.0,  example=1)
    orders_last_180d:        float = Field(1.0,  example=2)
    avg_days_between_orders: float = Field(0.0,  example=45.0)
    days_since_last_order:   float = Field(30.0, example=60)

class ChurnResponse(BaseModel):
    churn_probability: float
    is_churned:        bool
    risk_level:        str

class LeadRequest(BaseModel):
    origin:                  str   = Field(..., example="organic_search")
    first_contact_month:     float = Field(1.0, example=6)
    first_contact_dayofweek: float = Field(0.0, example=1)

class LeadResponse(BaseModel):
    conversion_probability: float
    is_converted:           bool
    recommendation:         str

class BatchDeliveryRequest(BaseModel):
    records: list[DeliveryRequest]

class BatchChurnRequest(BaseModel):
    records: list[ChurnRequest]

class BatchLeadRequest(BaseModel):
    records: list[LeadRequest]

class HealthResponse(BaseModel):
    status: str
    models: dict[str, bool]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _require(key: str):
    if key not in models:
        raise HTTPException(503, f"Model '{REGISTRY[key]}' not loaded. Check MLflow.")
    return models[key]


def _risk_level(p: float) -> str:
    if p >= 0.7:
        return "high"
    if p >= 0.4:
        return "medium"
    return "low"


def _recommendation(p: float) -> str:
    if p >= 0.6:
        return "High priority — assign account executive"
    if p >= 0.3:
        return "Medium priority — nurture with email campaign"
    return "Low priority — add to general newsletter"


def _delivery_df(records: list[DeliveryRequest]) -> pd.DataFrame:
    rows = []
    for r in records:
        d = r.model_dump()
        d["seller_customer_same_state"] = int(d["seller_state"] == d["customer_state"])
        rows.append(d)
    return pd.DataFrame(rows)[DELIVERY_COLS]


def _churn_df(records: list[ChurnRequest]) -> pd.DataFrame:
    return pd.DataFrame([r.model_dump() for r in records])[CHURN_COLS]


def _lead_df(records: list[LeadRequest]) -> pd.DataFrame:
    return pd.DataFrame([r.model_dump() for r in records])[LEAD_COLS]


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["meta"])
def health():
    return {
        "status": "ok" if len(models) == 3 else "degraded",
        "models": {k: k in models for k in REGISTRY},
    }


@app.get("/models", tags=["meta"])
def list_models():
    return {
        "models": [
            {
                "key":      k,
                "name":     REGISTRY[k],
                "loaded":   k in models,
                "endpoint": f"/predict/{'delivery-delay' if k == 'delivery' else k}",
            }
            for k in REGISTRY
        ]
    }


@app.post("/predict/delivery-delay", response_model=DeliveryResponse, tags=["predict"])
def predict_delivery(req: DeliveryRequest):
    model = _require("delivery")
    df    = _delivery_df([req])
    delay = float(model.predict(df)[0])
    if delay < 0:
        interp = f"{abs(delay):.1f} days early"
    elif delay > 0:
        interp = f"{delay:.1f} days late"
    else:
        interp = "on time"
    return {"delay_days": round(delay, 2), "interpretation": interp}


@app.post("/predict/churn", response_model=ChurnResponse, tags=["predict"])
def predict_churn(req: ChurnRequest):
    model = _require("churn")
    df    = _churn_df([req])
    prob  = float(model.predict_proba(df)[0][1])
    return {
        "churn_probability": round(prob, 4),
        "is_churned":        prob >= 0.5,
        "risk_level":        _risk_level(prob),
    }


@app.post("/predict/lead", response_model=LeadResponse, tags=["predict"])
def predict_lead(req: LeadRequest):
    model = _require("lead")
    df    = _lead_df([req])
    prob  = float(model.predict_proba(df)[0][1])
    return {
        "conversion_probability": round(prob, 4),
        "is_converted":           prob >= 0.5,
        "recommendation":         _recommendation(prob),
    }


@app.post("/predict/delivery-delay/batch", tags=["predict"])
def predict_delivery_batch(req: BatchDeliveryRequest):
    model  = _require("delivery")
    df     = _delivery_df(req.records)
    delays = model.predict(df).tolist()
    return {
        "predictions": [
            {
                "delay_days":     round(d, 2),
                "interpretation": (
                    f"{abs(d):.1f} days {'early' if d < 0 else 'late'}" if d != 0 else "on time"
                ),
            }
            for d in delays
        ]
    }


@app.post("/predict/churn/batch", tags=["predict"])
def predict_churn_batch(req: BatchChurnRequest):
    model = _require("churn")
    df    = _churn_df(req.records)
    probs = model.predict_proba(df)[:, 1].tolist()
    return {
        "predictions": [
            {
                "churn_probability": round(p, 4),
                "is_churned":        p >= 0.5,
                "risk_level":        _risk_level(p),
            }
            for p in probs
        ]
    }


@app.post("/predict/lead/batch", tags=["predict"])
def predict_lead_batch(req: BatchLeadRequest):
    model = _require("lead")
    df    = _lead_df(req.records)
    probs = model.predict_proba(df)[:, 1].tolist()
    return {
        "predictions": [
            {
                "conversion_probability": round(p, 4),
                "is_converted":           p >= 0.5,
                "recommendation":         _recommendation(p),
            }
            for p in probs
        ]
    }
