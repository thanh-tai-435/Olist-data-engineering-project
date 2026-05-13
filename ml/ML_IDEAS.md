# ML Ideas cho Olist Data Lakehouse

> Dataset: ~100K đơn hàng, 8 bảng ecom + 2 bảng marketing funnel  
> Stack: MLflow (tracking + serving) + Spark (feature engineering) + Gold layer (input data)  
> Mục tiêu: Production-grade ML pipelines tích hợp vào Medallion Architecture

---

## Tổng quan theo độ ưu tiên

| # | Use Case | Loại | Độ khó | Business Value | Ưu tiên |
|---|----------|------|--------|----------------|---------|
| 1 | Delivery Delay Prediction | Regression | ⭐⭐ | ★★★★★ | P0 |
| 2 | Customer Churn Prediction | Binary Classification | ⭐⭐ | ★★★★★ | P0 |
| 3 | Lead Scoring (Funnel) | Binary Classification | ⭐⭐ | ★★★★☆ | P0 |
| 4 | Customer Segmentation (RFM) | Clustering | ⭐⭐ | ★★★★☆ | P1 |
| 5 | Review Sentiment Analysis | NLP | ⭐⭐⭐ | ★★★★☆ | P1 |
| 6 | Product Demand Forecasting | Time Series | ⭐⭐⭐ | ★★★★☆ | P1 |
| 7 | Seller Performance Scoring | Scoring | ⭐⭐ | ★★★☆☆ | P2 |
| 8 | Fraud / Anomaly Detection | Anomaly Detection | ⭐⭐⭐ | ★★★★☆ | P2 |
| 9 | Product Recommendation | Collaborative Filtering | ⭐⭐⭐⭐ | ★★★☆☆ | P3 |
| 10 | Revenue Forecasting | Time Series | ⭐⭐⭐ | ★★★★★ | P2 |

---

## P0 — Core Models (phải có)

### 1. Delivery Delay Prediction

**Bài toán**: Dự đoán đơn hàng có bị giao trễ không, và trễ bao nhiêu ngày.

**Input** (từ `gold.fct_orders` + `gold.dim_sellers`):
```
seller_state, customer_state, product_category,
product_weight_g, freight_value, order_month,
distance_seller_to_customer (engineered)
```

**Output**: `actual_delivery_days - estimated_delivery_days` (regression) hoặc `is_late` (binary)

**Model**: XGBoost Regressor → XGBoost Classifier  
**Metric**: RMSE / F1-score  
**MLflow**: Log feature importance, shap values, confusion matrix  
**Serving**: REST API endpoint `/predict/delivery-delay`

**Giá trị demo**: Hiển thị real-time trên Streamlit dashboard khi đơn mới được tạo (streaming pipeline).

---

### 2. Customer Churn Prediction

**Bài toán**: Khách hàng có mua lại trong 90 ngày tới không?

**Input** (từ `gold.dim_customers` — tính theo snapshot date):
```
days_since_last_order, total_orders, avg_order_value,
avg_review_score, total_spend, product_category_diversity,
preferred_payment_type, customer_state
```

**Output**: Binary — `will_churn` (1 = không mua lại trong 90 ngày)

**Model**: LightGBM → Logistic Regression (baseline comparison)  
**Metric**: AUC-ROC, Recall (ưu tiên recall — bắt đúng churn)  
**MLflow**: Log ROC curve, feature importance, threshold analysis  
**Serving**: Batch inference job chạy daily, kết quả lưu vào `gold.customer_churn_scores`

**Giá trị demo**: Segment customers theo risk tier (High/Medium/Low churn) → hiển thị trên BI dashboard.

---

### 3. Lead Scoring (Marketing Funnel)

**Bài toán**: Lead nào có khả năng convert thành deal cao nhất?

**Input** (từ `gold.fct_funnel`):
```
origin (paid/organic/...), business_segment, lead_type,
has_company, has_gtin, average_stock,
days_from_first_contact_to_deal (target leak → exclude),
landing_page_id, sr_id (seller region)
```

**Output**: Probability score [0, 1] — khả năng lead → closed deal

**Model**: Random Forest → Gradient Boosting  
**Metric**: Precision@K, AUC-ROC  
**MLflow**: Track conversion rate per segment, log decision threshold  
**Serving**: Score mới leads real-time qua API `/predict/lead-score`

**Giá trị demo**: Giúp sales team ưu tiên follow-up → tăng conversion rate.

---

## P1 — Enhanced Models

### 4. Customer Segmentation — RFM + Clustering

**Bài toán**: Phân nhóm khách hàng theo hành vi mua hàng để personalization.

**Phương pháp**:
1. Tính RFM scores: Recency, Frequency, Monetary từ `fct_orders`
2. KMeans clustering (k=4~6) trên normalized RFM
3. Gán nhãn business: Champions / Loyal / At-Risk / Lost

**Input**: `customer_id`, `last_order_date`, `order_count`, `total_spend`  
**Output**: `segment_label`, `rfm_score`, `cluster_id`

**Model**: KMeans + PCA (visualization)  
**MLflow**: Log silhouette score, inertia curve (elbow method), cluster centroids  
**Output table**: `gold.customer_segments` — join được với churn model

**Giá trị demo**: 2D PCA scatter plot màu theo segment trên Streamlit.

---

### 5. Review Sentiment Analysis

**Bài toán**: Phân tích cảm xúc của review text → bổ sung cho review score (1-5).

**Input** (từ `bronze.ecom.reviews`):
```
review_comment_title, review_comment_message (tiếng Bồ Đào Nha)
```

**Output**: `sentiment` (positive/neutral/negative) + `confidence_score`

**Model Options**:
- Baseline: TextBlob / VADER (không cần train)
- Nâng cao: Fine-tune multilingual BERT (`bert-base-multilingual-cased`)
- Pragmatic: Google Translate API → sentiment trên tiếng Anh

**MLflow**: Log accuracy vs review_score correlation, confusion matrix  
**Giá trị demo**: Tag reviews tự động → "sentiment diverges from score" (review 4 sao nhưng sentiment negative = signal quan trọng).

**Lưu ý**: ~60% reviews không có text → handle NULL, train chỉ trên có text.

---

### 6. Product Demand Forecasting (per Category)

**Bài toán**: Dự đoán số đơn hàng theo category trong 30/60/90 ngày tới.

**Input**: Daily/weekly aggregate từ `gold.fct_orders`:
```
order_date, product_category, order_count, revenue
```

**Model**:
- Facebook Prophet (seasonal decomposition, holiday effects)
- ARIMA / SARIMA (baseline)
- LightGBM với lag features (production-grade)

**Output**: Forecast table `gold.demand_forecast` với confidence intervals

**MLflow**: Log MAPE, RMSE per category, log forecast plots  
**Giá trị demo**: Interactive forecast chart trên Streamlit — chọn category, xem dự đoán 90 ngày.

---

## P2 — Advanced Models

### 7. Seller Performance Scoring

**Bài toán**: Rank và score sellers để identify top performers và underperformers.

**Input** (từ `gold.dim_sellers` + aggregates):
```
avg_delivery_days, on_time_rate, avg_review_score,
total_orders, cancellation_rate, product_diversity,
avg_freight_value, seller_state
```

**Output**: `performance_score` [0-100] + `tier` (Platinum/Gold/Silver/Bronze)

**Model**: Weighted scoring formula + unsupervised ranking (không cần label)  
**Bonus**: Dùng Isolation Forest để detect outlier sellers (fraud-adjacent)  
**MLflow**: Log scoring weights, tier distribution, correlation heatmap

---

### 8. Fraud / Anomaly Detection

**Bài toán**: Phát hiện orders/sellers bất thường (fake reviews, order manipulation).

**Signals**:
```
# Order-level
unusually_high_freight_vs_distance
review_posted_before_delivery
same_customer_multiple_orders_same_minute

# Seller-level
sudden_spike_in_5star_reviews
review_score_vs_delivery_performance_divergence
```

**Model**:
- Isolation Forest (unsupervised, không cần label)
- Autoencoder (nếu muốn deep learning)
- One-Class SVM (baseline)

**MLflow**: Log anomaly scores, contamination parameter, flagged entity count  
**Output**: `gold.anomaly_flags` table — feed vào BI alert dashboard

---

### 10. Revenue Forecasting

**Bài toán**: Dự đoán tổng revenue tuần/tháng tới cho toàn platform.

**Input**: Weekly revenue time series từ 2016-09 đến 2018-08 (`fct_orders`)  
**Model**: Prophet + LightGBM ensemble  
**MLflow**: Log forecast vs actual (backtesting), confidence intervals  
**Serving**: Scheduled batch job → kết quả feed vào executive dashboard

---

## P3 — Nice-to-have

### 9. Product Recommendation

**Bài toán**: "Khách hàng mua X thường cũng mua Y" (Market Basket Analysis).

**Challenges với Olist**:
- 96% khách hàng chỉ mua 1 lần → collaborative filtering rất sparse
- Phù hợp hơn: Content-based (product category similarity)

**Approach thực tế**:
1. Association Rules (Apriori) trên order_items → `frequently_bought_together`
2. Item2Vec trên purchase sequences
3. Fallback: "Popular in your state" cho new users

**MLflow**: Log support/confidence/lift cho association rules

---

## Architecture: ML trong Medallion

```
Gold Layer (fct_orders, dim_customers, fct_funnel)
    │
    ▼
Feature Store (gold.feature_store_*)   ← Spark feature engineering jobs
    │
    ▼
MLflow Training (train_*.py)           ← Log experiments, register models
    │
    ▼
MLflow Model Registry
    ├── /Production  ← mlflow models serve
    └── /Staging     ← A/B testing
    │
    ▼
Serving Layer
    ├── REST API (FastAPI wrapper)      ← real-time predictions
    ├── Batch Inference (Spark)         ← daily scoring jobs
    └── gold.ml_predictions_*          ← kết quả lưu vào Iceberg
    │
    ▼
BI Layer (Streamlit / Metabase)        ← hiển thị predictions
```

---

## File Structure đề xuất

```
ml/
├── ML_IDEAS.md                        # file này
├── feature_engineering/
│   ├── features_delivery.py           # feature pipeline cho model 1
│   ├── features_churn.py              # feature pipeline cho model 2
│   └── features_funnel.py             # feature pipeline cho model 3
├── training/
│   ├── train_delivery_model.py        # P0
│   ├── train_churn_model.py           # P0
│   ├── train_lead_scoring.py          # P0
│   ├── train_segmentation.py          # P1
│   ├── train_sentiment.py             # P1
│   └── train_demand_forecast.py       # P1
├── inference/
│   ├── batch_inference.py             # Spark batch scoring
│   └── serve_model.py                 # FastAPI + MLflow serving
├── evaluation/
│   └── model_report.py                # MLflow comparison dashboard
└── notebooks/
    └── eda_for_ml.ipynb               # EDA trước khi train
```

---

## Gợi ý implement theo thứ tự

```
Week 1: train_delivery_model.py + serve_model.py (REST API)
Week 2: train_churn_model.py + batch_inference.py (daily Spark job)
Week 3: train_lead_scoring.py + tích hợp vào Prefect pipeline
Week 4: train_segmentation.py + visualization trên Streamlit
Week 5+: sentiment, forecasting (tùy thời gian còn lại)
```

**Mỗi model phải có**:
- [ ] Feature engineering script (đọc từ Gold layer)
- [ ] Training script với MLflow tracking (params, metrics, artifacts)
- [ ] Model registered vào MLflow Model Registry
- [ ] Inference endpoint hoặc batch job
- [ ] Kết quả predictions lưu vào Iceberg Gold table
