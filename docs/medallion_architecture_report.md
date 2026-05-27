# Kiến Trúc Dữ Liệu Medallion: Bronze – Silver – Gold trên Apache Iceberg và PySpark

---

## 1. Tổng quan Kiến trúc Medallion

Trong bối cảnh khối lượng dữ liệu doanh nghiệp ngày càng tăng trưởng theo cấp số nhân, các tổ chức phải đối mặt với thách thức dung hòa hai nhu cầu trái chiều: (1) lưu trữ dữ liệu thô, toàn vẹn để hỗ trợ tái xử lý (*replayability*); và (2) cung cấp dữ liệu đã được làm sạch, tổng hợp cho các hệ thống phân tích và học máy ở độ trễ thấp (Reis & Housley, 2022). Kiến trúc Data Lakehouse—sự hội tụ giữa Data Lake và Data Warehouse—đã nổi lên như một mô hình giải quyết bài toán này (Armbrust et al., 2021).

Bên trong mô hình Lakehouse, **kiến trúc Medallion** (còn gọi là *Multi-hop architecture*) tổ chức dữ liệu thành các tầng chất lượng tăng dần, ký hiệu bằng kim loại quý: Bronze, Silver và Gold (Reis & Housley, 2022, tr. 219–224). Mỗi tầng đại diện cho một cấp độ xử lý và một cam kết chất lượng dữ liệu rõ ràng:

| Tầng | Bí danh | Chất lượng | Tính bất biến | Người dùng chính |
|------|---------|-----------|--------------|-----------------|
| Bronze | Raw / Landing | Thô, nguyên trạng | Append-only | Data Engineer |
| Silver | Cleaned / Conformed | Đã làm sạch, đúng kiểu | Incremental merge | Analyst, Data Scientist |
| Gold | Curated / Serving | Tổng hợp, sẵn sàng BI | Full refresh / partitioned | BI, ML, Executive |

Armbrust et al. (2021) lập luận rằng mô hình Lakehouse—và theo đó kiến trúc Medallion—có thể đạt được hiệu năng truy vấn ngang ngửa Data Warehouse truyền thống trong khi vẫn duy trì tính linh hoạt của Data Lake, nhờ ba thành phần cốt lõi: định dạng bảng mở hỗ trợ ACID, lớp metadata tách biệt với lưu trữ, và engine truy vấn hiệu năng cao.

Trong hệ thống đồ án này, kiến trúc Medallion được triển khai trên **Apache Iceberg** (định dạng bảng), **Cloudflare R2** (lưu trữ đối tượng), và **Apache PySpark 3.5** (engine xử lý), điều phối bởi **Prefect** như một nền tảng orchestration hiện đại.

---

## 2. Tầng Bronze – Thu nạp Dữ liệu Thô

### 2.1 Nguyên tắc thiết kế

Tầng Bronze thực hiện một nguyên tắc duy nhất: **lưu trữ dữ liệu đúng như nguồn gốc** (*source-aligned*, Reis & Housley, 2022, tr. 220). Dữ liệu được nạp vào dưới dạng *append-only*—không cập nhật, không xóa—đảm bảo khả năng tái xử lý (*replayability*) toàn bộ lịch sử. Kleppmann (2017, tr. 460–462) nhấn mạnh rằng log bất biến (*immutable log*) là nền tảng của các hệ thống dữ liệu đáng tin cậy vì nó tách biệt ghi (*write*) khỏi đọc (*read*) và cho phép phục hồi sau lỗi mà không mất thông tin.

### 2.2 Triển khai với PyIceberg

Trong đồ án, tầng Bronze sử dụng **PyIceberg** để ghi 10 bảng từ tập dữ liệu Olist Brazilian E-Commerce (Olist, 2018) vào Iceberg trên Cloudflare R2:

- **Thương mại điện tử (8 bảng):** `ecommerce_orders`, `ecommerce_order_items`, `ecommerce_order_payments`, `ecommerce_order_reviews`, `ecommerce_products`, `ecommerce_sellers`, `ecommerce_customers`, `ecommerce_category_translation`
- **Marketing (2 bảng):** `marketing_leads`, `marketing_deals`

Quá trình nạp thực hiện các bước sau:

1. **Đọc CSV** từ thư mục `data/` bằng `pandas.read_csv()`.
2. **Chuyển đổi kiểu** timestamp sang `datetime64[us]` (microseconds) để tương thích với schema Iceberg, vì Apache Arrow—cầu nối giữa pandas và PyIceberg—chỉ hỗ trợ độ phân giải microsecond cho kiểu `TIMESTAMP` trong Iceberg (Apache Software Foundation, 2024a).
3. **Ghi append** vào bảng Iceberg đã tồn tại, hoặc tạo bảng mới nếu chưa có, thông qua REST Catalog (`http://iceberg-rest:8181`).

Metadata của mỗi bảng Iceberg (snapshot, manifest, schema) được lưu trong `s3://retail-data-lake/<namespace>/<table>/metadata/`, tách biệt hoàn toàn với data files (`.parquet`), đây là đặc điểm kiến trúc quan trọng của Iceberg (Apache Software Foundation, 2024b).

### 2.3 Kết quả

Sau quá trình ingestion, tổng cộng **1.559.693 bản ghi** được nạp thành công vào tầng Bronze, phân bổ trên 10 bảng Iceberg, với `_ingested_at` là timestamp ghi nhận thời điểm nạp dữ liệu—trường metadata này đóng vai trò quan trọng cho việc xử lý incremental ở các tầng sau.

---

## 3. Tầng Silver – Làm sạch và Chuẩn hóa Dữ liệu

### 3.1 Nguyên tắc thiết kế

Tầng Silver áp dụng các phép biến đổi *source-conforming*: làm sạch kiểu dữ liệu, loại bỏ bản ghi trùng lặp, chuẩn hóa chuỗi ký tự, và tính toán các trường dẫn xuất (*derived fields*) có tính tái sử dụng cao (Reis & Housley, 2022, tr. 221). Mục tiêu không phải là phục vụ trực tiếp nhu cầu nghiệp vụ, mà là tạo ra một nguồn dữ liệu "đáng tin cậy" (*trusted*) cho toàn bộ hạ tầng phía sau—một khái niệm mà Inmon (2005, tr. 29) gọi là *single version of the truth*.

Kimball và Ross (2013, tr. 20–23) phân biệt rõ hai loại bảng trong tầng này: **staging tables** (bảng trung gian, 1-1 với nguồn, chỉ làm sạch) và **intermediate tables** (bảng liên kết, kết hợp nhiều staging tables để tạo ra view denormalized phục vụ tầng Gold).

### 3.2 Công cụ xử lý: Apache PySpark

Hệ thống sử dụng **Apache PySpark 3.5** để xử lý tầng Silver. Zaharia et al. (2016) mô tả Spark như một "unified engine" cho xử lý dữ liệu lớn, với **Resilient Distributed Datasets (RDD)** làm abstraction cơ bản, cho phép thực thi in-memory và chịu lỗi (*fault-tolerant*) thông qua cơ chế lineage. Mặc dù tập dữ liệu Olist (~200MB) nhỏ hơn ngưỡng thông thường của Spark, việc sử dụng `local[2]` mode (Spark giả lập phân tán trên một máy với 2 CPU cores) đảm bảo tính nhất quán về API và khả năng scale-out khi cần mà không phải thay đổi code (Zaharia et al., 2016, tr. 57).

SparkSession được cấu hình với:

```
spark.sql.catalog.olist.type        = rest
spark.sql.catalog.olist.uri         = http://iceberg-rest:8181
spark.sql.catalog.olist.io-impl     = org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.olist.client.region = us-east-1   # dummy region cho R2
spark.sql.shuffle.partitions        = 8             # tối ưu cho dataset nhỏ
```

Cấu hình sử dụng hai bộ JAR độc lập: (1) `hadoop-aws` + `aws-java-sdk-bundle` (AWS SDK v1, cho `s3a://` filesystem); (2) `iceberg-aws-bundle` (AWS SDK v2, cho `S3FileIO` của Iceberg)—tương ứng hai lớp IO độc lập theo thiết kế của Apache Iceberg (Apache Software Foundation, 2024b).

### 3.3 Các phép biến đổi chính

**Bảng staging (9 bảng):**

| Bảng | Phép biến đổi nổi bật |
|------|-----------------------|
| `stg_orders` | Cast timestamp, tính `actual_delivery_days`, `estimated_delivery_days`; dedup theo `order_id` |
| `stg_order_payments` | Aggregate: 1 row/order; lấy `payment_type` với `payment_sequential = 1` bằng `F.first(F.when(...))` |
| `stg_order_reviews` | Dedup: lấy review mới nhất/order dùng `Window.partitionBy("order_id").orderBy(review_answer_timestamp DESC)` |
| `stg_products` | Tính `product_volume_cm3 = length × height × width`; fill null category bằng `"unknown"` |
| `stg_sellers` | Chuẩn hóa: `seller_city` → lowercase + trim; `seller_state` → uppercase |
| `stg_customers` | Chuẩn hóa tương tự sellers; dedup theo `customer_id` |
| `stg_marketing_leads` | Fill null `origin` → `"unknown"`; dedup theo `mql_id` |
| `stg_marketing_deals` | Cast `declared_monthly_revenue` → double; fill nulls |

**Bảng intermediate (1 bảng):**

`int_orders_enriched` là bảng denormalized kết hợp `stg_orders` + `stg_customers` + items aggregate + `stg_order_payments` + `stg_order_reviews`, bổ sung trường `delivery_delay_days = actual_delivery_days − estimated_delivery_days`. Đây là bảng trung gian được thiết kế theo triết lý "build once, use many" của Kimball và Ross (2013, tr. 468)—Gold layer chỉ cần đọc bảng này thay vì thực hiện lại các phép join phức tạp.

### 3.4 Chiến lược ghi: `createOrReplace`

Phương thức `df.writeTo(table).createOrReplace()` thực hiện *full refresh*—tạo snapshot Iceberg mới thay thế toàn bộ nội dung bảng. Điều này đảm bảo idempotency: chạy lại pipeline bất kỳ số lần vẫn cho kết quả nhất quán—tính chất mà Kleppmann (2017, tr. 478) xem là yêu cầu bắt buộc cho các pipeline batch xử lý không trạng thái (*stateless*).

---

## 4. Tầng Gold – Tổng hợp và Phục vụ Phân tích

### 4.1 Nguyên tắc thiết kế

Tầng Gold chứa dữ liệu được tổng hợp, partition hợp lý, và tối ưu cho truy vấn phân tích. Theo mô hình của Kimball và Ross (2013, tr. 27–52), dữ liệu tầng Gold được tổ chức theo **mô hình chiều** (*dimensional model*) với hai loại bảng:

- **Fact tables** (`fct_*`): chứa sự kiện đo lường được (đơn hàng, hành trình funnel), thường là bảng lớn, được partition theo thời gian.
- **Dimension tables** (`dim_*`): chứa thuộc tính mô tả thực thể nghiệp vụ (người bán, khách hàng), thường nhỏ hơn, full refresh.

### 4.2 Các bảng Gold được tạo ra

**`fct_orders` – Fact bán hàng:**

Mỗi dòng đại diện một đơn hàng, kế thừa từ `int_orders_enriched`. Các trường quan trọng:
- `delivery_status`: phân loại `early / on_time / late / unknown` dựa trên `delivery_delay_days`.
- `order_revenue`, `order_freight`, `payment_value`: dùng `F.coalesce(..., F.lit(0.0))` thay thế `NULL` bằng 0—thực hành chuẩn khi đưa dữ liệu vào BI tools (Kimball & Ross, 2013, tr. 73).
- **Partition:** `F.months("purchased_at")`—Iceberg Hidden Partitioning loại bỏ partition path trong câu query, tránh lỗi *partition column leakage* phổ biến trong Hive (Apache Software Foundation, 2024b).

**`fct_funnel` – Fact marketing funnel:**

Left join từ `stg_marketing_leads` sang `stg_marketing_deals`, tính `days_to_close = won_date − first_contact_date` và flag `is_converted` (0/1). Partition theo `F.years("first_contact_date")` vì dữ liệu funnel thưa hơn theo thời gian.

**`dim_sellers` – Chiều người bán:**

Aggregation metrics theo `seller_id`: `total_orders`, `total_revenue`, `avg_review_score`, `first_sale_date / last_sale_date`, `delivered_orders`. Left join với `stg_sellers` để bổ sung thông tin địa lý. Bảng không partition (kích thước nhỏ).

**`dim_customers` – Chiều khách hàng với CLV metrics:**

Đây là bảng phức tạp nhất do logic deduplication:

1. **Dedup `stg_customers`** theo `customer_unique_id`: do thiết kế của Olist, mỗi đơn hàng tạo một `customer_id` mới (one-time buyers), trong khi `customer_unique_id` đại diện người dùng thực. Dùng `Window.partitionBy("customer_unique_id").orderBy(customer_id DESC)` lấy `customer_id` mới nhất.

2. **Tính order metrics** theo `customer_unique_id`: join `stg_orders` với `stg_customers` (để lấy `customer_unique_id`) rồi join với `stg_order_payments`, aggregate `total_orders`, `total_spend`, `avg_order_value`, `days_since_last_order`.

3. **Feature engineering cho ML:**
   - `is_churned = 1` nếu `days_since_last_order > 90` (ngưỡng churn 90 ngày, phổ biến trong e-commerce, xem Fader & Hardie, 2005).
   - `is_repeat_customer = 1` nếu `total_orders > 1`.

### 4.3 Kết quả

| Bảng Gold | Số dòng | Partition |
|-----------|---------|-----------|
| `fct_orders` | ~99.441 | Theo tháng (`purchased_at`) |
| `fct_funnel` | ~8.000 | Theo năm (`first_contact_date`) |
| `dim_sellers` | ~3.095 | Không |
| `dim_customers` | ~96.096 | Không |

---

## 5. Apache Iceberg: Định dạng Bảng Mở

**Apache Iceberg** là một định dạng bảng mở (*open table format*) được thiết kế để giải quyết các giới hạn của Hive table format trong môi trường lưu trữ đối tượng (Apache Software Foundation, 2024b). Armbrust et al. (2020) chỉ ra ba vấn đề cốt lõi mà các định dạng bảng thế hệ mới (Iceberg, Delta Lake, Hudi) giải quyết:

1. **ACID transactions trên object store:** S3/R2 không hỗ trợ atomic operations; Iceberg giải quyết bằng *optimistic concurrency control* thông qua layer metadata.
2. **Schema evolution:** thêm/đổi tên cột không phá vỡ query cũ.
3. **Time travel:** `SELECT * FROM table VERSION AS OF <snapshot-id>` cho phép audit và rollback.

Trong đồ án, Iceberg REST Catalog (`tabulario/iceberg-rest`) đóng vai trò trung gian giữa Spark (engine) và R2 (storage), cho phép nhiều engine khác nhau (Spark, Trino, DuckDB, PyIceberg) đọc/ghi cùng bảng mà không conflict—đây là nguyên tắc *engine independence* của Lakehouse (Armbrust et al., 2021, tr. 3).

---

## 6. Lưu trữ trên Cloudflare R2

**Cloudflare R2** là dịch vụ lưu trữ đối tượng tương thích S3 API, với điểm khác biệt quan trọng là **zero egress fee** (Cloudflare, 2024)—phù hợp cho mô trường học thuật và prototype khi chi phí băng thông là yếu tố hạn chế. Về mặt kỹ thuật, R2 không yêu cầu `path-style access` (khác một số S3-compatible khác) và chấp nhận bất kỳ giá trị `region` nào (ví dụ `us-east-1` dùng như placeholder cho AWS SDK v2).

Cấu trúc lưu trữ trong bucket `retail-data-lake`:

```
retail-data-lake/
├── bronze/
│   ├── ecommerce_orders/
│   │   ├── data/         ← Parquet files
│   │   └── metadata/     ← Iceberg snapshot, manifest, schema JSON
│   └── ...
├── silver/
│   └── ...
└── gold/
    ├── fct_orders/
    │   └── data/
    │       ├── purchased_at_month=2017-01/
    │       └── purchased_at_month=2017-02/
    └── ...
```

Việc tách biệt `data/` và `metadata/` là thiết kế nền tảng của Iceberg, cho phép engine thực hiện *metadata-only queries* (đếm snapshot, liệt kê partition) mà không cần scan data files—cải thiện đáng kể hiệu năng với bảng lớn (Apache Software Foundation, 2024b; Armbrust et al., 2021).

---

## 7. Orchestration với Prefect

Pipeline Bronze → Silver → Gold được điều phối bởi **Prefect**, một nền tảng orchestration thế hệ thứ ba (*third-generation orchestrator*) theo cách phân loại của Reis và Housley (2022, tr. 317). Prefect sử dụng decorator `@flow` và `@task` để biến hàm Python thông thường thành đơn vị có thể quan sát, retry, và schedule, không yêu cầu cấu hình DAG riêng biệt như Apache Airflow. Mỗi tầng là một Prefect flow riêng:

- `bronze_ingestion_flow` → gọi PyIceberg writer
- `silver_transform_flow` → khởi động PySpark `local[2]`, chạy `run_silver()`
- `gold_transform_flow` → khởi động PySpark `local[2]`, chạy `run_gold()`
- `full_pipeline_flow` → chuỗi ba flow trên với dependency rõ ràng

---

## Tài liệu Tham khảo

Apache Software Foundation. (2024a). *PyIceberg: Python library for Apache Iceberg* (Version 0.6). https://py.iceberg.apache.org/

Apache Software Foundation. (2024b). *Apache Iceberg documentation* (Version 1.5). https://iceberg.apache.org/docs/1.5.0/

Armbrust, M., Das, T., Sun, L., Yavuz, B., Zhu, S., Murthy, M., Torres, J., van Hovell, H., Ionescu, A., Łuszczak, A., Switakowski, M., Szafrański, M., Li, X., Ueshin, T., Mokhtar, M., Boncz, P., Ghodsi, A., Paranjpye, S., Senster, P., & Zaharia, M. (2020). Delta Lake: High-performance ACID table storage over cloud object stores. *Proceedings of the VLDB Endowment, 13*(12), 3411–3424. https://doi.org/10.14778/3415478.3415560

Armbrust, M., Ghodsi, A., Xin, R., & Zaharia, M. (2021). Lakehouse: A new generation of open platforms that unify data warehousing and advanced analytics. *Proceedings of the 11th Annual Conference on Innovative Data Systems Research (CIDR '21)*. https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf

Cloudflare. (2024). *Cloudflare R2 storage: Zero egress fees*. https://www.cloudflare.com/developer-platform/r2/

Fader, P. S., & Hardie, B. G. S. (2005). The value of simple models in new product forecasting and customer-base analysis. *Marketing Science, 24*(4), 621–635. https://doi.org/10.1287/mksc.1050.0131

Inmon, W. H. (2005). *Building the data warehouse* (4th ed.). Wiley Technology Publishing.

Kimball, R., & Ross, M. (2013). *The data warehouse toolkit: The definitive guide to dimensional modeling* (3rd ed.). Wiley.

Kleppmann, M. (2017). *Designing data-intensive applications: The big ideas behind reliable, scalable, and maintainable systems*. O'Reilly Media.

Olist. (2018). *Brazilian E-Commerce public dataset by Olist* [Data set]. Kaggle. https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Reis, J., & Housley, M. (2022). *Fundamentals of data engineering: Plan and build robust data systems*. O'Reilly Media.

Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A., Meng, X., Rosen, J., Venkataraman, S., Franklin, M. J., Ghodsi, A., Gonzalez, J., Shenker, S., & Stoica, I. (2016). Apache Spark: A unified engine for big data processing. *Communications of the ACM, 59*(11), 56–65. https://doi.org/10.1145/2934664

---

*Ghi chú: Tài liệu này được soạn thảo theo chuẩn trích dẫn APA 7th Edition (American Psychological Association, 2020). Các URL truy cập lần cuối vào tháng 5 năm 2026.*
