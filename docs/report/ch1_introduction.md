# CHƯƠNG 1. GIỚI THIỆU ĐỀ TÀI

## 1.1 Bối cảnh và lý do chọn đề tài

Trong vòng một thập niên trở lại đây, thương mại điện tử đã chuyển dịch từ một kênh bán hàng bổ trợ sang nền tảng giao dịch cốt lõi của nền kinh tế số. Tại Brazil — thị trường thương mại điện tử lớn nhất Mỹ Latinh — Olist, nền tảng kết nối hàng nghìn nhà bán lẻ với người tiêu dùng, xử lý hơn 100.000 đơn hàng trên phạm vi 27 tiểu bang. Mỗi đơn hàng sinh ra hàng chục sự kiện dữ liệu: từ thời điểm đặt hàng, phê duyệt thanh toán, vận chuyển, giao hàng đến đánh giá của khách — tạo nên một luồng dữ liệu liên tục, đa chiều và giàu giá trị phân tích.

Thực tế này đặt ra yêu cầu kỹ thuật rõ ràng: hạ tầng dữ liệu phải xử lý **đồng thời** cả dữ liệu lịch sử lẫn sự kiện thời gian thực, đảm bảo tính nhất quán, hỗ trợ schema thay đổi linh hoạt, và phục vụ được nhiều workload — từ batch analytics đêm đến dashboard realtime trong ngày.

Các kiến trúc truyền thống gặp khó khăn trước yêu cầu này:

- **Data Warehouse** (PostgreSQL, Redshift): đảm bảo ACID và truy vấn phân tích tốt, nhưng schema-on-write cứng nhắc — mỗi thay đổi cấu trúc bảng đòi hỏi migration script và downtime. Tích hợp streaming phức tạp, chi phí lưu trữ cao hơn nhiều so với object storage.
- **Data Lake thuần túy** (raw Parquet trên S3): linh hoạt và chi phí thấp, nhưng thiếu ACID — khi nhiều pipeline ghi đồng thời, xung đột có thể làm hỏng dữ liệu một phần. Không hỗ trợ cập nhật/xóa tự nhiên, dễ biến thành "data swamp" sau vài tháng vận hành.

Sự ra đời của mô hình **Unified Lakehouse** — kết hợp tính linh hoạt của Data Lake với tính nhất quán của Data Warehouse — thông qua các open table format như Apache Iceberg đã mở ra hướng tiếp cận mới. Iceberg cung cấp ACID transactions, schema evolution không phá vỡ pipeline, time travel và hidden partitioning trực tiếp trên file storage chi phí thấp. Netflix (người tạo ra Iceberg), Apple, Uber và Airbnb đã triển khai kiến trúc này ở quy mô petabyte trong môi trường production, chứng minh tính khả thi.

Tuy nhiên, tài liệu học thuật về triển khai một Unified Lakehouse **end-to-end hoàn chỉnh** — từ catalog initialization, batch và streaming ingestion, automated transformation qua các tầng chất lượng dữ liệu, orchestration tự động, đến serving analytics — trên stack open-source miễn phí hoàn toàn vẫn còn rất hạn chế. Đề tài được thực hiện nhằm lấp đầy khoảng trống đó, xây dựng và kiểm chứng một nền tảng Lakehouse hoàn chỉnh trên dữ liệu thực từ Olist với Apache Iceberg, Redpanda, PySpark và Prefect — tất cả khởi động bằng một lệnh `docker compose up`.

---

## 1.2 Bài toán nghiên cứu

Đề tài tập trung xây dựng nền tảng dữ liệu hợp nhất giải quyết đồng thời ba nhóm bài toán:

**Bài toán 1 — Xử lý đa luồng dữ liệu:**
Hệ thống phải tiếp nhận dữ liệu theo hai luồng song song không xung đột:
- *Batch*: 10 bảng CSV (~200 MB, ~100K đơn hàng) nạp định kỳ vào storage
- *Streaming*: sự kiện đơn hàng real-time phát từ message broker, ghi vào cùng Bronze tables với ACID guarantees

**Bài toán 2 — Chất lượng và tổ chức dữ liệu:**
Dữ liệu thô chứa nhiều vấn đề: timestamp sai kiểu, giá trị null không mong muốn, duplicate records, đơn vị không nhất quán. Hệ thống cần tự động làm sạch, chuẩn hóa, join và xây dựng các analytical mart sẵn sàng cho BI — theo quy trình tái lặp được, idempotent.

**Bài toán 3 — Orchestration và vận hành:**
Toàn bộ pipeline từ ingestion đến transformation phải vận hành tự động, có retry khi lỗi, giám sát từng bước, và phục hồi từ điểm lỗi mà không chạy lại toàn bộ. Môi trường phát triển phải nhất quán giữa các thành viên nhóm, bất kể máy cá nhân.

---

## 1.3 Mục tiêu đề tài

### Mục tiêu chức năng

| # | Mục tiêu | Tiêu chí thành công |
|---|----------|---------------------|
| F1 | Unified Lakehouse 3 tầng Bronze/Silver/Gold trên Apache Iceberg | Đủ 3 namespace trên Cloudflare R2, dữ liệu truy vấn được qua PySpark |
| F2 | Pipeline batch ingest 10 bảng CSV → Bronze | Tất cả bảng xuất hiện trong Iceberg catalog sau mỗi lần chạy |
| F3 | Pipeline streaming: producer → Redpanda → consumer → Bronze | Consumer ghi dữ liệu realtime vào Bronze không xung đột với batch |
| F4 | Transformation tự động Bronze→Silver→Gold bằng PySpark | Bảng Gold (fct_orders, fct_funnel, dim_sellers, dim_customers) đúng schema, partitioned |
| F5 | Orchestrate toàn bộ pipeline bằng Prefect | Full pipeline chạy end-to-end từ Prefect UI, có artifacts, retry tự động |
| F6 | Dashboard realtime đọc sự kiện từ Redpanda | Streamlit refresh mỗi 3 giây, hiển thị live metrics |

### Mục tiêu kỹ thuật

| # | Mục tiêu | Giải pháp |
|---|----------|-----------|
| T1 | ACID writes đồng thời trên Bronze | Iceberg Optimistic Concurrency Control |
| T2 | Metadata bền vững qua restart | PostgreSQL-backed Iceberg REST Catalog |
| T3 | Transformation chạy cả local lẫn cluster không đổi code | Biến môi trường `SPARK_MASTER` |
| T4 | Toàn bộ stack khởi động bằng một lệnh | Docker Compose với profile-based services |
| T5 | Tunnel URL tự động cập nhật khi restart | Service `url-reporter` patch `.env` và Coder DB |

---

## 1.4 Dataset và phạm vi nghiên cứu

### Dataset

**Olist E-Commerce Dataset** (Kaggle, CC BY-NC-SA 4.0):

| Bảng | Mô tả | Số dòng |
|------|-------|---------|
| `olist_orders_dataset` | Đơn hàng, trạng thái, timestamp đầy đủ | ~99.441 |
| `olist_order_items_dataset` | Chi tiết sản phẩm trong từng đơn | ~112.650 |
| `olist_order_payments_dataset` | Phương thức và số tiền thanh toán | ~103.886 |
| `olist_order_reviews_dataset` | Đánh giá và phản hồi khách hàng | ~99.224 |
| `olist_products_dataset` | Danh mục và thuộc tính sản phẩm | ~32.951 |
| `olist_sellers_dataset` | Thông tin seller theo bang | ~3.095 |
| `olist_customers_dataset` | Thông tin khách hàng theo địa lý | ~99.441 |
| `olist_geolocation_dataset` | Tọa độ mã ZIP theo bang | ~1.000.163 |

**Marketing Funnel Dataset** (cùng nguồn, 2018):

| Bảng | Mô tả | Số dòng |
|------|-------|---------|
| `olist_marketing_qualified_leads` | Leads đủ điều kiện theo kênh | ~8.000 |
| `olist_closed_deals` | Leads chuyển đổi thành seller | ~842 |

Tổng dung lượng raw: ~200 MB. Phạm vi thời gian: 9/2016 – 10/2018.

### Phạm vi nghiên cứu

**Trong phạm vi (Phần C — Data Engineering):**
- Toàn bộ Data Platform layer: batch ingestion, streaming ingestion, transformation Bronze→Silver→Gold
- Orchestration tự động với Prefect
- Storage layer: Apache Iceberg trên Cloudflare R2
- Môi trường triển khai: Docker Compose, Coder cloud IDE
- Dashboard realtime: Streamlit đọc trực tiếp từ Redpanda

**Ngoài phạm vi (thuộc phần khác của nhóm):**
- Agentic BI — Natural Language to SQL via LLM
- Query federation với Trino
- Data governance và lineage (Apache Atlas, OpenMetadata)
- Production deployment trên cloud cluster

---

## 1.5 Cấu trúc báo cáo

Báo cáo được tổ chức thành 6 chương theo luồng từ lý thuyết đến thực nghiệm:

**Chương 1** — Bối cảnh, bài toán, mục tiêu và phạm vi đề tài. Xác lập rõ ranh giới giữa phần Data Engineering (Phần C) và các phần khác của đồ án nhóm.

**Chương 2 — Cơ sở lý thuyết**: Trình bày các nền tảng lý luận: Unified Lakehouse, Medallion Architecture, Apache Iceberg, xử lý streaming với Redpanda, transformation pipeline với PySpark, và DuckDB cho analytics. Đây là cơ sở cho các quyết định thiết kế ở Chương 3.

**Chương 3 — Phân tích và thiết kế hệ thống**: Phân tích yêu cầu chức năng/phi chức năng, thiết kế kiến trúc tổng thể 5 lớp, mô hình dữ liệu (ERD và Star Schema), và thiết kế chi tiết từng pipeline từ batch đến streaming đến transformation.

**Chương 4 — Xây dựng và triển khai hệ thống**: Trình bày chi tiết cài đặt từng thành phần — môi trường Docker Compose, Bronze ingestion với PyIceberg, streaming producer/consumer với Redpanda, Silver/Gold transformation với PySpark, Prefect orchestration, và Streamlit realtime dashboard.

**Chương 5 — Đánh giá hệ thống**: Đánh giá kết quả đạt được so với mục tiêu, đo lường hiệu năng query và streaming latency, so sánh với kiến trúc truyền thống, và nhận định về các hạn chế hiện tại.

**Chương 6 — Kết luận và hướng phát triển**: Tổng kết thành quả và đề xuất hướng mở rộng trong tương lai: distributed deployment, query federation, semantic layer, cloud-native architecture.
