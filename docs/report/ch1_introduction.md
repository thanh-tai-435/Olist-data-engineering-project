# CHƯƠNG 1. GIỚI THIỆU ĐỀ TÀI

## 1.1 Bối cảnh và lý do chọn đề tài

Thương mại điện tử toàn cầu đã và đang trải qua giai đoạn tăng trưởng bùng nổ trong thập kỷ vừa qua. Theo báo cáo của eMarketer (2023), doanh thu thương mại điện tử toàn cầu đạt hơn 5,8 nghìn tỷ USD và dự kiến tiếp tục tăng trưởng ở mức hai chữ số mỗi năm. Tại Brazil — thị trường mà bộ dữ liệu Olist đại diện — thương mại điện tử đã ghi nhận hàng triệu giao dịch mỗi năm, tạo ra một lượng dữ liệu khổng lồ về hành vi mua sắm, hiệu suất vận chuyển, đánh giá sản phẩm và hành trình chuyển đổi khách hàng từ marketing đến mua hàng.

Sự tăng trưởng này đặt ra yêu cầu ngày càng cao đối với hạ tầng dữ liệu. Các doanh nghiệp không chỉ cần lưu trữ lịch sử hàng triệu đơn hàng mà còn cần phân tích xu hướng theo thời gian thực, phát hiện bất thường ngay khi xảy ra, và phục vụ nhiều nhóm người dùng khác nhau — từ data analyst truy vấn dashboard hàng ngày đến data scientist huấn luyện mô hình dự đoán.

Kiến trúc dữ liệu truyền thống đối mặt với nhiều hạn chế trong bối cảnh này:

- **Data Warehouse thuần túy** (PostgreSQL, Redshift): lưu trữ tốt dữ liệu có cấu trúc, hỗ trợ ACID và analytical query nhanh, nhưng schema cứng nhắc (schema-on-write), chi phí lưu trữ cao, khó tích hợp dữ liệu streaming, và gặp khó khăn khi schema nguồn thay đổi theo thời gian.

- **Data Lake thuần túy** (raw S3/HDFS): lưu trữ linh hoạt với chi phí thấp, chấp nhận mọi định dạng dữ liệu, nhưng thiếu ACID transactions dẫn đến inconsistency khi có concurrent write, không có schema enforcement khiến dữ liệu dễ bị "swamp", và khó thực hiện update/delete (vốn là yêu cầu thường xuyên trong GDPR compliance).

Những hạn chế này thúc đẩy sự ra đời của kiến trúc **Unified Lakehouse** — một paradigm kết hợp ưu điểm của cả hai: lưu trữ rẻ trên object storage như Data Lake, nhưng bổ sung lớp metadata với ACID transactions, schema enforcement và query performance tốt như Data Warehouse. Đây là xu hướng đang được các tổ chức lớn như Databricks, Netflix, Apple và Airbnb áp dụng ở quy mô production với hàng petabyte dữ liệu.

Tuy nhiên, tài liệu và ví dụ triển khai thực tế về Unified Lakehouse — đặc biệt là kết hợp batch ingestion, streaming realtime, transformation pipeline tự động và dashboard analytics trong một hệ thống thống nhất — vẫn còn hạn chế ở cấp độ học thuật và sinh viên. Phần lớn tài liệu hiện có hoặc quá lý thuyết, hoặc chỉ trình bày từng thành phần riêng lẻ mà không có cái nhìn end-to-end.

Đây là lý do chính để đề tài này được thực hiện: xây dựng và triển khai một nền tảng Unified Lakehouse hoàn chỉnh từ ingestion đến serving, sử dụng bộ công nghệ mã nguồn mở hiện đại, trên một bộ dữ liệu thực từ ngành thương mại điện tử.

---

## 1.2 Bài toán nghiên cứu

Xét một doanh nghiệp thương mại điện tử quy mô vừa với các đặc điểm điển hình:

- **Nguồn dữ liệu đa dạng**: đơn hàng, sản phẩm, người bán, đánh giá khách hàng, dữ liệu marketing funnel — mỗi nguồn có schema riêng, tần suất cập nhật khác nhau.
- **Yêu cầu realtime**: bộ phận operations cần xem trạng thái đơn hàng mới trong vài giây, không phải sau khi batch pipeline chạy xong vào sáng hôm sau.
- **Yêu cầu lịch sử**: bộ phận analytics cần truy vấn hàng triệu đơn hàng lịch sử để tính KPI tháng/quý, so sánh year-over-year.
- **Schema thay đổi theo thời gian**: đội product thêm field mới vào đơn hàng, đội data không muốn viết migration script phức tạp mỗi lần.
- **Audit và compliance**: cần khả năng xem lại trạng thái dữ liệu tại một thời điểm cụ thể trong quá khứ (time travel), ví dụ để điều tra sự cố.
- **Chất lượng dữ liệu**: dữ liệu thô từ nhiều nguồn có thể có null, duplicate, giá trị ngoài phạm vi — cần phát hiện và xử lý trước khi dùng cho analytics.
- **Tự động hóa**: pipeline phải chạy tự động hàng ngày, có retry khi lỗi, có alerting khi fail — không cần người vận hành can thiệp thủ công thường xuyên.

Bài toán nghiên cứu được phát biểu như sau:

> **Làm thế nào để xây dựng một nền tảng dữ liệu hợp nhất có khả năng đồng thời tiếp nhận dữ liệu batch lịch sử và sự kiện streaming realtime, đảm bảo tính nhất quán và chất lượng dữ liệu, phục vụ analytical query nhanh, và tự động vận hành toàn bộ pipeline — tất cả trên open-source stack với chi phí triển khai thấp?**

Đề tài tiếp cận bài toán này thông qua việc xây dựng một hệ thống hoàn chỉnh sử dụng bộ dữ liệu Olist E-Commerce (~100K đơn hàng) như một proxy cho môi trường production thực tế.

---

## 1.3 Mục tiêu đề tài

### Mục tiêu chức năng

**MTC-01**: Xây dựng Unified Lakehouse với Medallion Architecture gồm 3 tầng Bronze (raw), Silver (cleaned), Gold (aggregated) trên Apache Iceberg.

**MTC-02**: Triển khai batch ingestion pipeline — đọc 10 bảng dữ liệu từ CSV, upload lên object storage, ghi vào Bronze Layer với ACID guarantees.

**MTC-03**: Triển khai streaming ingestion pipeline — replay lịch sử đơn hàng qua Redpanda message broker, consumer ghi vào Bronze Layer theo thời gian thực.

**MTC-04**: Tự động hóa toàn bộ pipeline (batch ingestion → quality check → transformation → gold layer) bằng workflow orchestration, có monitoring và retry tự động.

**MTC-05**: Đảm bảo chất lượng dữ liệu tại Bronze Layer bằng automated quality checks trước khi promote lên Silver.

**MTC-06**: Xây dựng analytics dashboard từ Gold Layer hiển thị các business metrics chính về doanh thu, hiệu suất người bán và hành vi khách hàng.

### Mục tiêu kỹ thuật

**MTK-01**: Sử dụng **Apache Iceberg** (format-version 2) với PyIceberg client cho toàn bộ CRUD operations — khai thác ACID transactions, Schema Evolution và Time Travel.

**MTK-02**: Lưu trữ tất cả Iceberg data/metadata trên **Cloudflare R2** (S3-compatible object storage) — chi phí thấp, egress miễn phí.

**MTK-03**: Xây dựng ELT transformation pipeline bằng **dbt** với incremental strategies phù hợp cho từng layer (append cho Bronze, merge cho Silver, full refresh partition cho Gold).

**MTK-04**: Triển khai streaming pipeline với **Redpanda** (Kafka-compatible, single container) và **confluent-kafka** Python client.

**MTK-05**: Sử dụng **DuckDB** (embedded, không cần server/container) làm analytical query engine cho dbt và dashboard — tận dụng columnar execution trực tiếp trên Iceberg Parquet files.

**MTK-06**: Orchestrate toàn bộ pipeline với **Prefect** — sử dụng `@flow`/`@task` decorators, parallel task execution, retry mechanism và Prefect Cloud UI cho monitoring.

**MTK-07**: Kiểm tra chất lượng dữ liệu với **Soda Core** — YAML-based checks tích hợp vào Prefect flow, fail task khi phát hiện vi phạm.

**MTK-08**: Toàn bộ hệ thống containerized bằng **Docker Compose** — khởi động với lệnh `docker compose up`, không cần cài đặt thủ công.

---

## 1.4 Dataset và phạm vi nghiên cứu

### Dataset

Đề tài sử dụng hai bộ dữ liệu công khai từ Kaggle, được cung cấp bởi Olist — một công ty thương mại điện tử Brazil:

**1. Olist E-Commerce Public Dataset**

Bộ dữ liệu gồm ~100.000 đơn hàng tại Brazil trong giai đoạn 2016–2018, với 8 bảng quan hệ:

| Bảng | Mô tả | Số dòng (ước lượng) |
|---|---|---|
| `olist_orders_dataset` | Thông tin đơn hàng, trạng thái, timestamps | ~100K |
| `olist_order_items_dataset` | Chi tiết sản phẩm trong đơn hàng | ~113K |
| `olist_order_payments_dataset` | Phương thức và giá trị thanh toán | ~104K |
| `olist_order_reviews_dataset` | Đánh giá và nhận xét của khách hàng | ~99K |
| `olist_customers_dataset` | Thông tin khách hàng, địa chỉ | ~99K |
| `olist_sellers_dataset` | Thông tin người bán, địa điểm | ~3K |
| `olist_products_dataset` | Danh mục và thuộc tính sản phẩm | ~33K |
| `olist_geolocation_dataset` | Bảng tra cứu mã bưu chính Brazil | ~1M |

**2. Olist Marketing Funnel Dataset**

Dữ liệu hành trình chuyển đổi từ lead đến giao dịch, gồm 2 bảng:

| Bảng | Mô tả | Số dòng (ước lượng) |
|---|---|---|
| `olist_marketing_qualified_leads_dataset` | Marketing Qualified Leads (MQL) | ~8K |
| `olist_closed_deals_dataset` | Leads đã chốt thành giao dịch | ~842 |

Tổng dung lượng dữ liệu raw: ~200MB. Mặc dù là small dataset, kiến trúc được thiết kế để scale lên TB-scale bằng cách thay DuckDB bằng Apache Spark mà không cần thay đổi Iceberg schema hay dbt models.

### Phạm vi công nghệ

Đề tài sử dụng bộ công nghệ sau:

| Layer | Công nghệ | Vai trò |
|---|---|---|
| Object Storage | Cloudflare R2 | Lưu trữ Iceberg data + metadata |
| Table Format | Apache Iceberg + PyIceberg | ACID, Time Travel, Schema Evolution |
| Streaming | Redpanda + confluent-kafka | Message broker + Python client |
| Transformation | dbt + DuckDB | ELT pipeline, analytical query |
| Quality | Soda Core | Data quality checks |
| Orchestration | Prefect | Workflow automation, monitoring |
| Serving | Streamlit | Analytics dashboard |
| DevOps | Docker Compose | Containerization |

### Ngoài phạm vi

Các thành phần sau nằm ngoài phạm vi của đề tài này, được để lại như hướng phát triển:

- **Apache Spark**: kiến trúc Iceberg đã sẵn sàng, có thể thêm Spark cluster sau khi dataset vượt 1TB.
- **Trino Query Federation**: join Iceberg + Postgres + CSV trong một query — đề cập trong hướng phát triển.
- **Machine Learning (MLflow)**: delivery delay prediction, churn model — phạm vi của Hướng 2.
- **Agentic BI (Claude API)**: natural language to SQL interface — phạm vi mở rộng.
- **Kubernetes deployment**: hệ thống hiện chạy Docker Compose single-node.

---

## 1.5 Cấu trúc báo cáo

Báo cáo được tổ chức theo 6 chương như sau:

**Chương 1 — Giới thiệu đề tài** (chương hiện tại): trình bày bối cảnh, bài toán nghiên cứu, mục tiêu, dataset và phạm vi.

**Chương 2 — Cơ sở lý thuyết**: trình bày các khái niệm và công nghệ nền tảng: Lambda Architecture, Medallion Architecture, Apache Iceberg, Streaming với Redpanda, ELT với dbt, DuckDB, Prefect Orchestration và Soda Core Data Quality.

**Chương 3 — Phân tích và thiết kế hệ thống**: phân tích functional/non-functional requirements, thiết kế kiến trúc tổng thể, data model (ERD + Star Schema), thiết kế các pipeline và orchestration flows.

**Chương 4 — Xây dựng và triển khai hệ thống**: trình bày chi tiết cài đặt và triển khai từng thành phần: Docker Compose environment, Iceberg table setup, batch và streaming ingestion, dbt transformation models, Prefect flows, Soda Core checks và Streamlit dashboard.

**Chương 5 — Đánh giá hệ thống**: trình bày kết quả đạt được, benchmark hiệu năng query và streaming, so sánh với kiến trúc truyền thống và phân tích hạn chế.

**Chương 6 — Kết luận và hướng phát triển**: tổng kết đóng góp của đề tài và đề xuất các hướng mở rộng trong tương lai.

---

*Hình 1.1 — Luồng dữ liệu tổng quát của hệ thống (từ nguồn CSV/Streaming đến Gold Layer và Dashboard) được trình bày chi tiết trong Chương 3.*
