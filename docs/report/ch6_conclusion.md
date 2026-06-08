# CHƯƠNG 6. KẾT LUẬN VÀ HƯỚNG PHÁT TRIỂN

## 6.1 Kết luận

Đề tài đã xây dựng thành công một nền tảng Unified Lakehouse hoàn chỉnh cho dữ liệu thương mại điện tử Olist, kết hợp Lambda Architecture (batch + streaming) với Medallion Architecture (Bronze → Silver → Gold) trên Apache Iceberg và Cloudflare R2.

### 6.1.1 Đóng góp kỹ thuật

**Về kiến trúc**: Đề tài chứng minh tính khả thi của việc xây dựng một production-grade data platform hoàn chỉnh bằng open-source stack với chi phí thấp — lưu trữ trên Cloudflare R2 (egress miễn phí), compute trên DuckDB (embedded, không server), orchestration trên Prefect Cloud (free tier). Toàn bộ chạy trong Docker Compose trên một machine thông thường.

**Về batch + streaming integration**: Đề tài minh họa cách Apache Iceberg ACID transactions cho phép batch ingestion (PyIceberg append từ CSV) và streaming ingestion (Redpanda consumer) ghi đồng thời vào cùng một Bronze table mà không có conflict — giải quyết điểm yếu cốt lõi của Data Lake truyền thống.

**Về ELT pipeline**: Mô hình ELT với dbt + DuckDB trên Iceberg cho thấy hiệu quả rõ rệt: transform logic được version control bằng SQL, incremental processing tiết kiệm compute, và built-in testing đảm bảo data quality tại Silver/Gold layer.

**Về data quality**: Tích hợp Soda Core vào Prefect flow tạo ra một quality gate tự động: dữ liệu xấu từ Bronze bị ngăn chặn trước khi promote lên Silver, đảm bảo Gold layer luôn đáng tin cậy cho analytics.

**Về observability**: Toàn bộ pipeline được monitor qua Prefect Cloud UI — run history, task logs, failure alerts — mà không cần SSH vào server. Đây là yếu tố quan trọng cho operability trong môi trường production.

### 6.1.2 Kết quả đo lường

- **10 Bronze tables** với tổng ~415K records, ACID-guaranteed, schema-evolved
- **dbt lineage**: 7 staging models + 2 intermediate + 4 Gold models, 100% tests pass
- **Query performance**: tất cả analytical queries < 3 giây trên 200MB dataset
- **Streaming latency**: end-to-end < 5 giây từ producer đến Bronze visible
- **Pipeline reliability**: retry mechanism hoạt động đúng trong cả 4 failure scenarios được test

### 6.1.3 Bài học kinh nghiệm

Quá trình triển khai hệ thống rút ra một số bài học quan trọng:

1. **Iceberg catalog persistence** là bước hay bị bỏ qua khi mới bắt đầu: REST catalog in-memory mất state khi restart. Production cần JDBC catalog với PostgreSQL backend.

2. **DuckDB + R2 network latency**: DuckDB nhanh khi dữ liệu local, nhưng khi đọc từ remote object storage, bottleneck là network, không phải CPU. Cần cân nhắc caching strategy cho dashboard có nhiều concurrent user.

3. **Streaming idempotency**: Consumer cần commit offset sau khi write Iceberg thành công (không dùng auto-commit). Duplicate do retry được xử lý ở Silver layer bằng merge strategy.

4. **Docker Compose healthcheck**: Healthcheck cần dùng tool có sẵn trong container (python3 urllib, bash TCP) thay vì curl — tránh tình trạng service unhealthy do curl không được cài.

---

## 6.2 Hướng phát triển

### 6.2.1 Distributed Processing với Apache Spark

Khi dataset vượt 1TB, DuckDB single-node sẽ không đủ. Kiến trúc Iceberg đã sẵn sàng: thêm Spark cluster (spark-master + spark-workers) vào Docker Compose, cập nhật dbt profile sang `dbt-spark`, và pipeline chạy distributed mà không cần thay đổi Silver/Gold SQL models.

Ưu tiên: khi dataset Silver vượt ~10GB hoặc khi cần xử lý streaming với throughput >10,000 events/giây.

### 6.2.2 Query Federation với Trino

Trino (PrestoSQL) cho phép join Iceberg Gold tables với các nguồn dữ liệu khác trong một query mà không cần ETL:

```sql
-- Join Iceberg Gold với Postgres CRM và CSV marketing data
SELECT o.order_id, o.revenue, c.campaign_name, p.customer_segment
FROM iceberg.gold.fct_orders o
JOIN postgres.crm.marketing_campaigns c ON o.seller_id = c.seller_id
JOIN hive.external.customer_segments p ON o.customer_id = p.customer_id
WHERE o.order_date >= DATE '2018-01-01'
```

Trino catalog configs đã có trong `trino/catalog/` — cần triển khai service và test federation queries.

### 6.2.3 Machine Learning với MLflow (Hướng 2)

Đây là hướng phát triển thành một đề tài riêng (Hướng 2) hoặc module mở rộng:

- **Delivery delay prediction**: features từ Gold Layer (`seller_state`, `customer_state`, `product_weight_g`, `freight_value`) → XGBoost model → dự đoán số ngày delay
- **Churn prediction**: features từ `dim_customers` (`days_since_last_order`, `total_orders`, `avg_order_value`) → logistic regression → identify at-risk customers
- **Lead scoring**: features từ `fct_funnel` → gradient boosting → rank leads by conversion probability

MLflow tracking server + model registry đã có trong Docker Compose. Training scripts cần viết thêm.

### 6.2.4 Agentic BI với Claude API

Thêm một Streamlit tab cho phép người dùng đặt câu hỏi bằng ngôn ngữ tự nhiên:

```
User: "Tháng nào có doanh thu cao nhất năm 2017?"
→ Claude API (claude-sonnet-4-6) sinh SQL
→ DuckDB execute trên Gold Layer
→ Hiển thị kết quả + biểu đồ
```

Module `bi/agentic_bi.py` đã có skeleton, cần tích hợp Anthropic SDK với context về Gold schema.

### 6.2.5 CI/CD với GitHub Actions

```yaml
# .github/workflows/dbt_test.yml
on: [pull_request]
jobs:
  dbt-test:
    steps:
      - run: dbt run --select silver.* --target ci
      - run: dbt test --target ci
      - run: soda scan -d bronze -c soda_checks.yml
```

Mỗi PR thay đổi dbt models sẽ trigger CI pipeline, ngăn deploy model lỗi vào production.

### 6.2.6 Data Governance

Khi hệ thống scale và có nhiều team dùng chung:
- **Apache Atlas / OpenMetadata**: data lineage tự động, column-level documentation, PII tagging
- **Row-level Security**: Iceberg row filter để giới hạn seller chỉ xem data của mình
- **Column masking**: ẩn PII (customer email, phone) với downstream consumers
- **Audit logging**: track ai query gì, khi nào — quan trọng cho LGPD (Brazil GDPR equivalent) compliance

### 6.2.7 Cloud-native Deployment

Migrate từ Docker Compose single-node sang Kubernetes:
- Prefect workers → Kubernetes CronJob
- Redpanda → Managed Kafka (Confluent Cloud / Redpanda Cloud)
- Iceberg catalog → AWS Glue Catalog hoặc Polaris Catalog
- DuckDB → MotherDuck (DuckDB as a Service) cho collaborative analytics

---

## Tài liệu tham khảo

[1] Apache Software Foundation. *Apache Iceberg Table Spec*. https://iceberg.apache.org/spec/

[2] Databricks. *Delta Lake vs Apache Iceberg vs Apache Hudi*. Databricks Engineering Blog, 2023.

[3] Nathan Marz, James Warren. *Big Data: Principles and Best Practices of Scalable Realtime Data Systems*. Manning Publications, 2015.

[4] Armbrust, M., et al. *Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics*. CIDR 2021.

[5] Redpanda Data. *Redpanda Documentation*. https://docs.redpanda.com/

[6] dbt Labs. *dbt Developer Guide*. https://docs.getdbt.com/

[7] DuckDB Foundation. *DuckDB Documentation*. https://duckdb.org/docs/

[8] Prefect Technologies. *Prefect 3 Documentation*. https://docs.prefect.io/

[9] Soda. *Soda Core Documentation*. https://docs.soda.io/soda-core/

[10] Olist. *Brazilian E-Commerce Public Dataset by Olist*. Kaggle, 2018. https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

[11] Olist. *Marketing Funnel by Olist*. Kaggle, 2019. https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist
