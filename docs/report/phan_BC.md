# **PHẦN B. XÂY DỰNG NỀN TẢNG UNIFIED LAKEHOUSE CHO PHÂN TÍCH DỮ LIỆU THƯƠNG MẠI ĐIỆN TỬ THỜI GIAN THỰC** {#phần-b}

---

## **CHƯƠNG 1. GIỚI THIỆU ĐỀ TÀI** {#chương-1-giới-thiệu}

### **1.1 Bối cảnh và lý do chọn đề tài** {#1.1-bối-cảnh}

Trong vòng một thập niên trở lại đây, thương mại điện tử đã và đang chuyển dịch từ một kênh kinh doanh hỗ trợ sang nền tảng giao dịch cốt lõi và gần như không thể thiếu trong nền kinh tế số. Sự mở rộng nhanh chóng của các nền tảng trực tuyến kéo theo lượng dữ liệu phát sinh ở quy mô chưa từng có, bao gồm dữ liệu đơn hàng, hành vi người dùng, thanh toán, logistics, chăm sóc khách hàng và tương tác marketing đa kênh. Khác với hệ thống dữ liệu truyền thống vốn chủ yếu xử lý dữ liệu định kỳ theo giờ hoặc theo ngày, các nền tảng thương mại điện tử hiện đại đòi hỏi hạ tầng dữ liệu có khả năng phản ứng kịp thời với những thay đổi liên tục. Trong môi trường cạnh tranh cao, việc chậm trễ vài phút trong phát hiện xu hướng doanh thu, tắc nghẽn vận chuyển hay suy giảm tỷ lệ chuyển đổi có thể gây ra tổn thất đáng kể về doanh thu và trải nghiệm khách hàng.

Trước thực trạng trên, phần lớn các hệ thống hạ tầng dữ liệu truyền thống hiện nay vẫn đang bộc lộ nhiều hạn chế khi đối mặt với yêu cầu dữ liệu động và đa nguồn. Kiến trúc Data Warehouse truyền thống (PostgreSQL, Redshift, SQL Server) tuy cung cấp hiệu năng truy vấn phân tích tốt và đảm bảo tính nhất quán dữ liệu thông qua ACID transactions, nhưng lại thiết kế theo mô hình schema-on-write cứng nhắc — mỗi khi cần thêm trường thông tin mới hay thay đổi cấu trúc bảng, đội kỹ thuật phải viết migration script, tạm ngừng pipeline và tải lại dữ liệu. Điều này đặc biệt gây khó khăn với các doanh nghiệp thương mại điện tử nơi schema nguồn thay đổi thường xuyên theo vòng phát triển sản phẩm. Bên cạnh đó, chi phí lưu trữ trên database storage cao hơn nhiều so với object storage, đồng thời việc tích hợp xử lý dữ liệu streaming vào Data Warehouse đòi hỏi xây dựng thêm lớp CDC (Change Data Capture) hay Debezium phức tạp và tốn kém.

Ở chiều ngược lại, kiến trúc Data Lake thuần túy (Hadoop HDFS, Amazon S3 raw) tuy linh hoạt trong lưu trữ và chi phí thấp, nhưng thiếu các cơ chế đảm bảo tính nhất quán cơ bản. Khi nhiều pipeline ghi đồng thời vào cùng một thư mục Parquet, không có cơ chế nào ngăn chặn xung đột ghi dẫn đến dữ liệu bị hỏng một phần. Việc cập nhật hay xóa các bản ghi cụ thể — vốn là yêu cầu thường xuyên trong tuân thủ quy định bảo mật dữ liệu — không được hỗ trợ tự nhiên trên raw file storage. Hệ quả là sau một thời gian vận hành, Data Lake dễ trở thành "data swamp" — dữ liệu tích lũy thiếu kiểm soát, không có schema enforcement, khó truy vấn và độ tin cậy thấp.

Sự phát triển của mô hình Unified Lakehouse trong những năm gần đây, đặc biệt là sự trưởng thành của các open table format như Apache Iceberg, đã mở ra hướng tiếp cận mới cho bài toán trên. Thay vì phải lựa chọn giữa tính linh hoạt của Data Lake và tính nhất quán của Data Warehouse, Unified Lakehouse kết hợp cả hai: lưu trữ dữ liệu dưới dạng file Parquet trên object storage chi phí thấp như Data Lake, nhưng bổ sung lớp metadata với ACID transactions, schema evolution tự động, time travel và partition pruning như Data Warehouse. Các tổ chức lớn như Netflix, Apple, Uber và Airbnb đã triển khai kiến trúc này ở quy mô petabyte trong môi trường production, chứng minh tính khả thi và hiệu quả của mô hình. Đặc biệt quan trọng, khi kết hợp với Lambda Architecture — mô hình xử lý đồng thời cả batch và streaming — kiến trúc Lakehouse cho phép dữ liệu từ pipeline hàng đêm và sự kiện thời gian thực hội tụ vào cùng một nguồn sự thật duy nhất mà không xung đột.

Tuy nhiên, tài liệu và ví dụ thực tiễn hoàn chỉnh về việc triển khai một nền tảng Unified Lakehouse end-to-end — từ khởi tạo catalog, xây dựng pipeline ingestion batch và streaming, automated transformation qua các tầng chất lượng dữ liệu, cho đến orchestration tự động và phục vụ analytics — vẫn còn rất hạn chế ở cấp độ học thuật và đặc biệt là các ví dụ có thể tái lập được trên môi trường open-source hoàn toàn miễn phí. Đề tài "XÂY DỰNG NỀN TẢNG UNIFIED LAKEHOUSE CHO PHÂN TÍCH DỮ LIỆU THƯƠNG MẠI ĐIỆN TỬ THỜI GIAN THỰC" được thực hiện nhằm lấp đầy khoảng trống này, xây dựng và kiểm chứng một nền tảng Lakehouse hoàn chỉnh trên bộ dữ liệu thực từ Olist với stack công nghệ mã nguồn mở Apache Iceberg, Redpanda, PySpark và Prefect — tất cả có thể khởi động chỉ với một lệnh `docker compose up`.

### **1.2 Mục tiêu nghiên cứu** {#1.2-mục-tiêu}

Đề tài hướng đến xây dựng một nền tảng dữ liệu hợp nhất có khả năng tiếp nhận và xử lý đồng thời dữ liệu lịch sử theo lô và sự kiện thời gian thực, đảm bảo tính nhất quán, tự động hóa toàn bộ quy trình và phục vụ được nhu cầu phân tích kinh doanh trực tiếp từ dữ liệu đã được tổ chức tốt.

Về mặt chức năng, đề tài tập trung vào việc xây dựng Unified Lakehouse theo mô hình Medallion Architecture gồm ba tầng Bronze, Silver và Gold trên Apache Iceberg, trong đó dữ liệu từ cả hai luồng batch và streaming đều hội tụ vào Bronze Layer với ACID guarantees nhờ cơ chế Optimistic Concurrency Control của Iceberg. Pipeline batch ingest 10 bảng CSV từ Cloudflare R2 vào Bronze, hỗ trợ chế độ append cho cập nhật tăng dần và chế độ overwrite để reset khi cần. Song song đó, pipeline streaming cho phép replay lịch sử đơn hàng qua Redpanda message broker với hai chế độ vận hành — chế độ rate phát đều đặn cho demo dashboard realtime và chế độ replay nén thời gian theo tỷ lệ gốc cho kiểm thử temporal ordering. Consumer đọc từ ba topics đồng thời và ghi vào Bronze bằng cơ chế dual flush kết hợp giữa batch size và time-based timeout, đảm bảo dữ liệu không bị giữ lại trong buffer khi lưu lượng thấp. Toàn bộ pipeline từ ingestion đến transformation được tự động hóa hoàn toàn thông qua Prefect với dependency management rõ ràng, retry tự động và Artifacts monitoring — không có bất kỳ bước nào yêu cầu can thiệp thủ công.

Về mặt kỹ thuật, đề tài hướng đến việc sử dụng Apache Iceberg format-version 2 với PostgreSQL JDBC Catalog nhằm đảm bảo metadata bền vững qua các lần restart, thay vì in-memory catalog chỉ phù hợp cho demo. Cloudflare R2 được chọn làm object storage backend nhờ egress miễn phí và tương thích S3 API hoàn toàn, giảm thiểu chi phí vận hành so với AWS S3. PySpark đảm nhiệm Silver và Gold transformation với thiết kế cho phép chuyển đổi linh hoạt giữa chế độ local và cluster chỉ thông qua một biến môi trường, không cần thay đổi code. Toàn bộ hệ thống được containerized bằng Docker Compose với kiến trúc profile-based — core services luôn hoạt động, optional services như Spark cluster, Trino và Streamlit được kích hoạt theo nhu cầu thực tế.

### **1.3 Đối tượng và phạm vi nghiên cứu** {#1.3-đối-tượng}

Đối tượng nghiên cứu của đề tài là nền tảng dữ liệu hợp nhất (Unified Lakehouse) được xây dựng cho bài toán phân tích thương mại điện tử, trong đó trọng tâm là lớp Data Platform — bao gồm toàn bộ vòng đời từ tiếp nhận dữ liệu thô theo cả hai luồng batch và streaming, xử lý theo Medallion Architecture qua ba tầng Bronze, Silver và Gold trên Apache Iceberg, đến orchestration pipeline tự động bằng Prefect. Đây cũng chính là phần công việc mà tác giả trực tiếp thiết kế, triển khai và kiểm thử trong dự án nhóm, phân biệt với lớp Intelligence Layer (Agentic BI, dashboard) do thành viên nhóm đảm nhận.

Về phạm vi dữ liệu, đề tài sử dụng hai bộ dữ liệu công khai từ Kaggle do Olist — nền tảng thương mại điện tử Brazil — cung cấp. Bộ thứ nhất, Olist E-Commerce Public Dataset, ghi lại khoảng 100.000 đơn hàng trong giai đoạn 2016–2018 với 8 bảng quan hệ (Bảng 1). Bộ thứ hai, Olist Marketing Funnel Dataset, theo dõi hành trình chuyển đổi từ Marketing Qualified Lead đến closed deal với 2 bảng (~8.700 bản ghi). Tổng dung lượng raw khoảng 200MB — nhỏ về kích thước nhưng đủ đa dạng về quan hệ để kiểm chứng các cơ chế Iceberg có ý nghĩa, trong khi vẫn chạy được trên môi trường laptop thông thường.

**Olist E-Commerce Public Dataset** — ~100.000 đơn hàng tại Brazil giai đoạn 2016–2018:

| Bảng CSV | Iceberg table | Số dòng xấp xỉ |
| :--- | :--- | :---: |
| `olist_orders_dataset.csv` | `bronze.ecommerce_orders` | ~100K |
| `olist_order_items_dataset.csv` | `bronze.ecommerce_order_items` | ~113K |
| `olist_order_payments_dataset.csv` | `bronze.ecommerce_order_payments` | ~104K |
| `olist_order_reviews_dataset.csv` | `bronze.ecommerce_order_reviews` | ~99K |
| `olist_customers_dataset.csv` | `bronze.ecommerce_customers` | ~99K |
| `olist_sellers_dataset.csv` | `bronze.ecommerce_sellers` | ~3K |
| `olist_products_dataset.csv` | `bronze.ecommerce_products` | ~33K |
| `olist_geolocation_dataset.csv` | `bronze.ecommerce_geolocation` | ~1M |

**Olist Marketing Funnel Dataset**:

| Bảng CSV | Iceberg table | Số dòng xấp xỉ |
| :--- | :--- | :---: |
| `olist_marketing_qualified_leads_dataset.csv` | `bronze.marketing_leads` | ~8K |
| `olist_closed_deals_dataset.csv` | `bronze.marketing_deals` | ~842 |

#### *Bảng 1: Dataset và Iceberg table mapping* {#bảng-1-dataset}

Về phạm vi công nghệ, đề tài tập trung vào stack mã nguồn mở hoàn toàn có thể tái lập được trên môi trường Docker Compose (Bảng 2). Cloudflare R2 được chọn làm object storage với egress miễn phí; Apache Iceberg format-version 2 với PostgreSQL JDBC Catalog đảm bảo metadata bền vững qua các lần khởi động lại container; Redpanda thay thế Kafka với footprint nhẹ hơn đáng kể; PySpark đảm nhiệm transformation; Prefect 3 điều phối toàn bộ pipeline. Nằm ngoài phạm vi của đề tài: Trino query federation, triển khai Spark cluster phân tán thực sự (chỉ kiểm thử ở chế độ local), huấn luyện model với MLflow và triển khai trên Kubernetes.

| Layer | Công nghệ | Phiên bản |
| :--- | :--- | :---: |
| Object Storage | Cloudflare R2 | — |
| Table Format | Apache Iceberg + PyIceberg | format-version 2 |
| Iceberg Catalog | PostgreSQL JDBC (tabulario/iceberg-rest) | 0.10.0 |
| Streaming | Redpanda + confluent-kafka | v23.3.18 |
| Processing | PySpark | 3.5.0 |
| Orchestration | Prefect | 3-latest |
| ML Tracking | MLflow | v2.13.0 |
| DevOps | Docker Compose | — |

#### *Bảng 2: Stack công nghệ và phiên bản* {#bảng-2-stack}

### **1.4 Phương pháp nghiên cứu** {#1.4-phương-pháp}

Đề tài áp dụng phương pháp nghiên cứu ứng dụng kết hợp thiết kế hệ thống và kiểm thử thực nghiệm. Quá trình bắt đầu từ việc tổng hợp tài liệu kỹ thuật về các cơ chế cốt lõi của Apache Iceberg (Optimistic Concurrency Control, snapshot isolation, hidden partitioning), mô hình Medallion Architecture từ tài liệu của Databricks và các case study triển khai production tại Netflix, Airbnb. Trên cơ sở lý thuyết đó, kiến trúc hệ thống được thiết kế theo nguyên tắc separation of concerns — mỗi tầng Bronze, Silver, Gold có trách nhiệm rõ ràng và giao tiếp qua Iceberg table interface — trước khi tiến hành triển khai.

Quá trình triển khai đi theo hướng bottom-up: infrastructure (Docker Compose, Iceberg catalog, R2 bucket) được dựng trước, tiếp theo là batch ingestion pipeline, rồi đến streaming producer/consumer, sau đó PySpark transforms và cuối cùng là Prefect orchestration. Mỗi lớp được kiểm thử riêng biệt — từ kiểm tra schema alignment của Iceberg tables đến integration test end-to-end pipeline từ CSV đến Gold Layer — trước khi kết hợp vào pipeline hoàn chỉnh. Hệ thống được đánh giá theo hai nhóm tiêu chí: tiêu chí chức năng (pipeline chạy end-to-end không lỗi, ACID guarantees hoạt động đúng, schema evolution không cần downtime) và tiêu chí hiệu năng (query latency dưới 5 giây trên Gold Layer, streaming end-to-end latency dưới 10 giây). Báo cáo được tổ chức theo vòng đời phát triển hệ thống: Chương 2 xây dựng nền tảng lý thuyết; Chương 3 trình bày thiết kế kiến trúc và mô hình dữ liệu; Chương 4 mô tả chi tiết quá trình triển khai; Chương 5 đánh giá kết quả và hiệu năng; Chương 6 kết luận và định hướng phát triển.

---

## **CHƯƠNG 2. CƠ SỞ LÝ THUYẾT** {#chương-2-lý-thuyết}

### **2.1 Unified Lakehouse** {#2.1-lakehouse}

#### **Data Warehouse**

Data Warehouse là hệ thống lưu trữ và xử lý được tối ưu hóa cho analytical workloads. Dữ liệu từ các hệ thống nguồn được extract, transform và load (ETL) vào DWH theo schema định nghĩa trước (schema-on-write). Kiến trúc columnar storage, pre-aggregated tables và materialized views giúp DWH trả lời analytical queries nhanh.

**Ưu điểm**: ACID transactions đảm bảo tính nhất quán; schema enforcement giúp governance; query performance cao với columnar indexing.

**Hạn chế**: Schema cứng nhắc — mỗi lần thêm column phải viết migration script và reload toàn bộ data; chi phí lưu trữ cao vì dùng database storage; không hỗ trợ tốt dữ liệu bán cấu trúc; tích hợp streaming phức tạp và tốn kém.

#### **Data Lake**

Data Lake lưu trữ tập trung toàn bộ dữ liệu ở dạng raw trên object storage chi phí thấp (S3, R2, GCS). Schema được áp dụng khi đọc (schema-on-read), không phải khi ghi — cho phép lưu bất kỳ loại dữ liệu nào mà không cần chuẩn bị schema trước.

**Ưu điểm**: Lưu trữ linh hoạt, chi phí thấp; hỗ trợ mọi loại và kích cỡ dữ liệu; decoupled storage và compute.

**Hạn chế**: Thiếu ACID transactions — hai writers đồng thời có thể corrupt data; không có schema enforcement nên dữ liệu dễ trở thành "data swamp"; không hỗ trợ row-level update/delete (vấn đề với GDPR).

#### **Unified Lakehouse**

Lakehouse là kiến trúc thống nhất kết hợp tính linh hoạt của Data Lake với tính nhất quán của Data Warehouse. Điểm cốt lõi là lớp **transaction protocol + metadata** được thêm vào phía trên object storage:

| Tiêu chí | Data Warehouse | Data Lake | Unified Lakehouse |
| :--- | :---: | :---: | :---: |
| ACID Transactions | ✓ | ✗ | ✓ |
| Schema Evolution | Khó | ✓ | ✓ |
| Streaming support | Khó | ✓ | ✓ |
| Query performance | Cao | Thấp | Cao |
| Chi phí lưu trữ | Cao | Thấp | Thấp |
| Time Travel | ✗ | ✗ | ✓ |

#### *Bảng 3: So sánh Data Warehouse, Data Lake và Unified Lakehouse* {#bảng-3-so-sánh}

### **2.2 Medallion Architecture** {#2.2-medallion}

Medallion Architecture tổ chức data lake/lakehouse thành các layer chất lượng tăng dần — Bronze, Silver, Gold:

**Bronze (Raw / Landing Zone)**: Lưu dữ liệu **raw và nguyên vẹn** như khi nhận từ nguồn. Append-only, immutable — là "nguồn sự thật gốc". Nếu Silver/Gold có lỗi, luôn có thể reprocess từ Bronze. Không có transform nào ở layer này; chỉ thêm metadata columns để tracking.

**Silver (Cleaned / Validated)**: Dữ liệu đã được cast đúng types, xử lý null, deduplicate, chuẩn hóa tên column. Strategy: incremental — chỉ xử lý delta từ lần chạy trước. Silver là layer dành cho data engineer và data scientist exploratory analysis.

**Gold (Business-Ready / Aggregated)**: Aggregated metrics theo business logic, pre-joined fact/dimension tables theo Star Schema, partitioned by time. Tối ưu hóa cho BI tools và analytical queries. Gold là nguồn dữ liệu duy nhất cho dashboard và reports.

**Lợi ích thực tiễn của phân tầng**:
* Khi Gold có số liệu sai, truy ngược về Silver rồi Bronze để xác định lỗi ở layer nào mà không cần chạy lại toàn bộ pipeline.
* Batch team ghi Bronze, data engineer transform Silver/Gold, analyst query Gold — độc lập nhau.
* Silver chỉ xử lý delta, giảm compute cost đáng kể khi dataset lớn.
* Bronze immutable đảm bảo reproducibility — có thể rebuild toàn bộ Lakehouse bất kỳ lúc nào từ nguồn gốc.

### **2.3 Apache Iceberg** {#2.3-iceberg}

Apache Iceberg là open table format cho large analytic datasets, ban đầu được phát triển bởi Netflix (2017) và hiện là Apache top-level project. Iceberg giải quyết các vấn đề cốt lõi của data lake trên object storage thông qua kiến trúc 3 tầng:

```
Data Files (.parquet)
    ↑
Manifest Files (danh sách data files + column statistics)
    ↑
Snapshot (trạng thái bảng tại một thời điểm — con trỏ đến manifest list)
```

Mỗi write operation tạo ra một Snapshot mới — atomic commit bằng cách swap con trỏ metadata. Cơ chế này đảm bảo:

* **ACID Transactions**: Dùng Optimistic Concurrency Control (OCC) — mỗi writer đọc current snapshot, thực hiện thay đổi, atomic commit. Nếu snapshot đã thay đổi (do concurrent writer), retry commit. Cho phép batch và streaming ghi đồng thời vào cùng table mà không corrupt.

* **Schema Evolution**: Thêm, đổi tên, xóa column mà không cần rewrite data files. Iceberg track schema changes trong metadata — downstream models không cần biết schema đã thay đổi cho đến khi chúng cần column đó.

* **Time Travel**: Mỗi Snapshot có timestamp và ID duy nhất. Có thể truy vấn bảng tại bất kỳ snapshot nào:
  ```sql
  SELECT * FROM orders FOR VERSION AS OF 1234567890
  SELECT * FROM orders FOR TIMESTAMP AS OF '2024-01-15 00:00:00'
  ```

* **Hidden Partitioning**: Partition logic nằm trong metadata, không lộ ra trong query. Analyst viết `WHERE order_date = '2018-01'` mà không cần biết partition scheme bên dưới.

**PyIceberg** là Python client cho phép CRUD operations (create namespace/table, append, merge, expire snapshots) trực tiếp từ Python mà không cần Spark hay Java runtime.

**Iceberg Catalog** lưu trữ metadata về tables (location, schema, partitions). Đề tài dùng **PostgreSQL JDBC Catalog** qua `tabulario/iceberg-rest` REST server — persistent storage trên PostgreSQL đảm bảo catalog không mất khi restart container.

### **2.4 Streaming và ELT Pipeline** {#2.4-streaming-elt}

#### **Streaming với Redpanda**

Redpanda là distributed streaming platform Kafka-compatible, được viết bằng C++ (không cần JVM), chạy trong single container mà không cần ZooKeeper. Trong kiến trúc đề tài, Redpanda đóng vai trò **Speed Layer**: tiếp nhận sự kiện đơn hàng, buffer và phân phối cho consumer ghi vào Bronze Iceberg.

Hai chế độ producer được xây dựng:
* **`rate` mode** (mặc định): gửi đúng `RATE` events/giây (mặc định 2/s), lặp vô hạn qua dataset, gán timestamp hiện tại. Phù hợp cho demo dashboard realtime.
* **`replay` mode**: sort events theo `order_purchase_timestamp` gốc, nén thời gian bằng `SPEED_FACTOR` (mặc định 86400 — 1 ngày = 1 giây). Phù hợp cho kiểm tra temporal ordering.

Consumer dùng dual flush mechanism: flush khi đủ `BATCH_SIZE` messages hoặc khi idle quá `FLUSH_INTERVAL` giây — đảm bảo dữ liệu không bị giữ lâu trong buffer khi traffic thấp.

#### **ELT Transformation với PySpark + Iceberg**

ELT (Extract-Load-Transform) là mô hình transform dữ liệu trong đó dữ liệu được load vào Lakehouse trước (Bronze), sau đó transform bên trong — tận dụng sức mạnh tính toán của distributed engine thay vì transform trên hệ thống nguồn.

Đề tài dùng **PySpark 3.5** với **Iceberg Spark Catalog** để thực hiện Silver và Gold transforms. PySpark đọc trực tiếp Bronze Iceberg tables từ R2 qua S3A connector, thực hiện transformations và ghi kết quả vào Silver/Gold Iceberg tables. `SPARK_MASTER` environment variable điều khiển chế độ: `local[2]` (dev) hoặc `spark://spark-master:7077` (production cluster).

### **2.5 Query và Dashboard Analytics** {#2.5-query-dashboard}

**DuckDB** là embedded OLAP engine (không cần server, không cần container) với vectorized execution engine. DuckDB đọc trực tiếp Parquet files từ Iceberg tables trên R2 qua S3 API, thực hiện columnar execution in-memory. Phù hợp cho analytical workloads trên dataset <1TB — với dataset Olist ~200MB, queries điển hình hoàn thành trong dưới 2 giây.

**Streamlit** là Python web framework cho data applications — không cần HTML/CSS/JavaScript, toàn bộ UI định nghĩa bằng Python thuần. Dashboard kết nối DuckDB với Gold Layer để render business metrics, charts và tables. Streamlit session state quản lý conversation history cho Agentic BI (thành phần do thành viên nhóm phát triển).

---

## **CHƯƠNG 3. PHÂN TÍCH VÀ THIẾT KẾ HỆ THỐNG** {#chương-3-thiết-kế}

### **3.1 Phân tích yêu cầu hệ thống** {#3.1-yêu-cầu}

#### **Functional Requirements**

**FR-01 — Batch Ingestion**: Hệ thống đọc 10 bảng CSV từ R2 bucket `retail-data-lake/raw/`, ghi vào Bronze Iceberg tables với ACID guarantees. Hỗ trợ chế độ append (mặc định) và overwrite (reset hoàn toàn).

**FR-02 — Streaming Ingestion**: Hệ thống replay dữ liệu lịch sử đơn hàng qua Redpanda, consumer đọc từ 3 topics (`olist.orders`, `olist.reviews`, `olist.payments`) và append vào Bronze tables tương ứng liên tục.

**FR-03 — Data Validation**: Validate schema và dữ liệu lỗi trước khi ghi vào Bronze. Phát hiện null trên primary key, type mismatch giữa message payload và Iceberg schema.

**FR-04 — ELT Transformation**: Hệ thống tự động transform Bronze → Silver (clean, type cast, dedup) → Gold (aggregate, Star Schema, partition by time) bằng PySpark.

**FR-05 — Pipeline Orchestration**: Toàn bộ pipeline chạy tự động với dependency management, retry tự động khi fail, monitoring qua Prefect UI — không cần can thiệp thủ công.

**FR-06 — Analytics Dashboard**: Dashboard hiển thị business metrics từ Gold Layer, cập nhật tự động sau mỗi pipeline run, hỗ trợ realtime metrics từ streaming.

**FR-07 — Containerized Deployment**: Toàn bộ hệ thống core chạy trong Docker Compose với `docker compose up`. Optional services (Spark cluster, Trino, Streamlit) khởi động qua `--profile` flags.

#### **Non-functional Requirements**

**NFR-01 — Append-only Bronze**: Bronze Layer tuyệt đối append-only trong normal operation. Chỉ có `--overwrite` flag cho phép reset, cần chạy script riêng — không thể xảy ra ngầm trong pipeline.

**NFR-02 — Low Latency**: Streaming end-to-end latency (produce → Bronze visible) < 5 giây. Query trên Gold Layer (200MB) < 5 giây với DuckDB.

**NFR-03 — Retry Mechanism**: Mỗi Prefect task có retry config phù hợp. Task fail sau tất cả retries → flow fail → Prefect UI hiển thị failure reason.

**NFR-04 — Scalability**: Khi dataset vượt 1TB, chuyển Spark từ `local[2]` sang cluster mode bằng một biến môi trường — không cần sửa code pipeline.

**NFR-05 — Schema Flexibility**: Thêm column mới vào Bronze schema (Iceberg Schema Evolution) mà không cần rewrite data files hay dừng pipeline đang chạy.

**NFR-06 — Observability**: Pipeline execution history, task-level logs, Artifacts (summary tables, dbt output) hiển thị trên Prefect Server UI — không cần SSH để debug.

### **3.2 Thiết kế kiến trúc tổng thể** {#3.2-kiến-trúc}

Hệ thống được thiết kế theo **Lambda Architecture** kết hợp **Medallion layering** trên Iceberg:

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│  [Olist CSV files on R2/raw/]   [CSV Replay → Redpanda]        │
└──────────┬──────────────────────────────┬───────────────────────┘
           │ Batch                        │ Stream
           ▼                              ▼
  ┌─────────────────┐          ┌──────────────────────┐
  │ batch_ingest_   │          │  streaming/          │
  │ bronze.py       │          │  producer.py         │
  │ (boto3 + pandas │          │  (mode: rate/replay) │
  │  + PyIceberg)   │          └──────────┬───────────┘
  └────────┬────────┘                     │
           │                   ┌──────────▼───────────┐
           │                   │       REDPANDA        │
           │                   │  olist.orders         │
           │                   │  olist.reviews        │
           │                   │  olist.payments       │
           │                   └──────────┬───────────┘
           │                              │
           │                   ┌──────────▼───────────┐
           │                   │  streaming/          │
           │                   │  consumer.py         │
           │                   │  (batch 200 msgs +   │
           │                   │   flush 15s timeout) │
           └──────────┬────────────────────┘
                      │
         ┌────────────▼─────────────┐
         │       BRONZE LAYER       │  ← Apache Iceberg on R2
         │  bronze.ecommerce_*      │    PostgreSQL JDBC Catalog
         │  bronze.marketing_*      │    ACID: concurrent write OK
         └────────────┬─────────────┘
                      │
         ┌────────────▼─────────────┐
         │    Data Validation       │  ← Schema check + null check
         │    (trước khi transform) │
         └────────────┬─────────────┘
                      │
         ┌────────────▼─────────────┐
         │       SILVER LAYER       │  ← PySpark (clean, type, dedup)
         │  silver.stg_orders       │    Iceberg incremental merge
         │  silver.stg_sellers ...  │
         └────────────┬─────────────┘
                      │
         ┌────────────▼─────────────┐
         │       GOLD LAYER         │  ← PySpark (aggregate, partition)
         │  gold.fct_orders         │    Star Schema
         │  gold.dim_sellers ...    │
         └────────────┬─────────────┘
                      │
         ┌────────────┴────────────┐
         ▼                         ▼
┌─────────────────┐     ┌──────────────────────┐
│ Streamlit        │     │  DuckDB              │
│ Dashboard        │     │  (query engine,      │
│ (realtime +      │     │   reads Iceberg      │
│  historical BI)  │     │   from R2 directly)  │
└─────────────────┘     └──────────────────────┘

[Prefect orchestrates Bronze → Silver → Gold với retry và monitoring]
```

**Ba nguyên tắc thiết kế cốt lõi:**

1. **Tách biệt storage và compute**: dữ liệu lưu trên R2, compute engines (PySpark, DuckDB, Prefect worker) chạy riêng — scale compute độc lập.
2. **Tách biệt chức năng theo layer**: ingest, transform, query là 3 concerns độc lập — lỗi ở layer này không phá vỡ layer kia.
3. **Immutable Bronze**: một khi ghi vào Bronze, dữ liệu không bao giờ bị sửa hay xóa trong normal operation.

### **3.3 Thiết kế dữ liệu** {#3.3-dữ-liệu}

#### **ERD — Bronze Layer**

Bronze tables phản chiếu 1:1 với CSV nguồn, thêm 3 metadata columns chuẩn:
* `_ingested_at TIMESTAMP`: UTC timestamp của lần ingest.
* `_source_file STRING`: tên file CSV (batch) hoặc `stream:{topic}` (streaming).
* `_source_path STRING`: đường dẫn đầy đủ — `s3://retail-data-lake/raw/...` (batch) hoặc `kafka://redpanda:9092/{topic}` (streaming).

Quan hệ giữa các Bronze tables phản ánh cấu trúc business:

```
ecommerce_customers (customer_id PK)
    ↑ 1:N
ecommerce_orders (order_id PK, customer_id FK)
    ↑ 1:N                        ↑ 1:N               ↑ 1:N
ecommerce_order_items      ecommerce_order_payments  ecommerce_order_reviews
(order_id FK, product_id FK)
    ↓ N:1
ecommerce_products (product_id PK)
    (category, weight, dimensions)

ecommerce_sellers (seller_id PK)
    ↑ referenced by order_items

marketing_leads (mql_id PK)
    ↑ 1:1
marketing_deals (mql_id FK)
```

#### **Star Schema — Gold Layer**

Gold được tổ chức theo Star Schema tối ưu cho analytical queries:

| Bảng | Loại | Grain | Partition |
| :--- | :---: | :--- | :--- |
| `gold.fct_orders` | Fact | 1 đơn hàng | `order_date_month` |
| `gold.fct_funnel` | Fact | 1 lead | — |
| `gold.dim_sellers` | Dimension | 1 người bán | — |
| `gold.dim_customers` | Dimension | 1 khách hàng | — |

#### *Bảng 4: Thiết kế Gold Layer* {#bảng-4-gold}

`fct_orders` chứa các pre-computed business metrics: `gross_revenue`, `net_revenue`, `delivery_delay_days` (actual - estimated), `is_late_delivery`, `avg_review_score`. Pre-compute ở Gold để analytical queries không cần join hay tính toán runtime.

### **3.4 Thiết kế pipeline** {#3.4-pipeline}

#### **Batch Pipeline**

```
[CSV files on R2: retail-data-lake/raw/ecommerce/ + raw/marketing/]
    → boto3 get_object() → pandas.read_csv()
    → timestamp cast (datetime64[ns] → datetime64[us])
    → thêm metadata columns (_ingested_at, _source_file, _source_path)
    → pa.Table.from_pandas()
    → PyIceberg table.append()  [Iceberg atomic commit]
    → bronze.ecommerce_* + bronze.marketing_*
```

10 bảng được ingest tuần tự. Mỗi bảng là một Iceberg table riêng tại `s3://retail-data-lake/bronze/{table_name}/`. Hỗ trợ `--overwrite` để drop và recreate table.

#### **Streaming Pipeline**

```
streaming/producer.py
    → load_events(): đọc orders + join customers + join payments
    → mode=rate: 2 events/s, loop vô hạn, timestamp=NOW
    → mode=replay: sort by _event_ts, sleep = data_elapsed / SPEED_FACTOR
    → Redpanda: olist.orders (3 partitions), olist.reviews (3), olist.payments (3)
    → streaming/consumer.py (consumer group: iceberg-bronze-writer)
    → buffer per topic
    → flush khi đủ BATCH_SIZE=200 HOẶC idle quá FLUSH_INTERVAL=15s
    → PyIceberg table.append()  [manual Kafka offset commit sau flush]
    → bronze.ecommerce_orders / ecommerce_order_reviews / ecommerce_order_payments
```

Consumer align DataFrame với Iceberg schema (thêm cột thiếu = null, bỏ cột thừa, reorder) trước khi append — đảm bảo tương thích khi schema Bronze và message payload có minor differences.

#### **Transformation Pipeline**

```
Bronze Iceberg (R2)
    → PySpark (Silver transform)
        → clean timestamps, handle nulls, deduplicate
        → stg_orders, stg_sellers, stg_customers, stg_products, ...
    → Silver Iceberg (R2)
    → PySpark (Gold transform)
        → join stg_* → fct_orders (partitioned by order_date_month)
        → dim_sellers, dim_customers, fct_funnel
    → Gold Iceberg (R2)
```

#### **Orchestration Flow**

Ba Prefect flows orchestrate toàn bộ: `bronze-batch-ingestion` → `silver-transform` → `gold-transform`. `full-pipeline` là entry point gọi 3 flows theo thứ tự tuần tự với params `skip_bronze` và `full_refresh`.

### **3.5 Thiết kế Dashboard Analytics** {#3.5-dashboard}

Dashboard được thiết kế theo hai luồng dữ liệu song song:

**Luồng historical analytics**: Gold Layer (Iceberg trên R2) → DuckDB đọc Parquet files trực tiếp → query results → Streamlit charts và tables. Metrics: doanh thu theo tháng/category/state, top sellers by revenue, delivery SLA compliance, customer retention.

**Luồng realtime monitoring**: Redpanda topic `olist.orders` → Kafka consumer với `@st.cache_resource` buffer (`deque(maxlen=2000)`) → derive realtime metrics → Streamlit auto-refresh mỗi 3 giây. Metrics: đơn hàng mới trong 60 giây qua, cumulative revenue, phân bố trạng thái đơn hàng.

---

## **CHƯƠNG 4. XÂY DỰNG VÀ TRIỂN KHAI HỆ THỐNG** {#chương-4-triển-khai}

### **4.1 Môi trường triển khai** {#4.1-môi-trường}

Toàn bộ hệ thống triển khai trên Docker Compose với kiến trúc profile-based: core services luôn chạy, optional services kích hoạt theo nhu cầu.

**Core services** (luôn chạy với `docker compose up`):

| Service | Image | Vai trò | Port |
| :--- | :--- | :--- | :---: |
| `postgres` | postgres:15-alpine | Backend cho Iceberg catalog, Prefect metadata, MLflow | 5432 |
| `iceberg-rest` | tabulario/iceberg-rest:0.10.0 | Iceberg REST Catalog (JDBC backend) | 8181 |
| `redpanda` | redpandadata/redpanda:v23.3.18 | Kafka-compatible streaming broker | 19092 |
| `redpanda-console` | redpandadata/console:v2.4.6 | Redpanda Web UI | 8080 |
| `redpanda-init` | (one-shot) | Tạo 5 topics khi khởi động | — |
| `prefect-server` | prefecthq/prefect:3-latest | Orchestration API + Web UI | 4200 |
| `prefect-worker` | Custom Python image | Thực thi Prefect flows | — |
| `mlflow` | ghcr.io/mlflow/mlflow:v2.13.0 | ML experiment tracking | 5000 |

#### *Bảng 5: Core Docker Compose services* {#bảng-5-docker}

**Optional profiles**:

| Profile | Services | Khi nào dùng |
| :--- | :--- | :--- |
| `--profile spark` | `spark-master` (port 8090), `spark-worker` (port 8091) | Dataset > 1TB, distributed processing |
| `--profile query` | `trino` (port 8082) | Query federation Iceberg + Postgres |
| `--profile bi` | `streamlit` (port 8501) | Agentic BI + dashboard |
| `--profile coder` | `coder`, `ngrok` | Team cloud IDE |

**Điểm quan trọng về Iceberg Catalog**: Đề tài dùng **PostgreSQL JDBC Catalog** (không phải in-memory). Iceberg REST server kết nối đến PostgreSQL database `iceberg` qua `CATALOG_URI: jdbc:postgresql://postgres:5432/iceberg`. Mọi table registration, schema và snapshot metadata được lưu persistent — catalog không mất khi restart container.

**Cloudflare Tunnel**: Ba tunnels (`cf-prefect`, `cf-mlflow`, `cf-redpanda`) luôn chạy, cung cấp URL công khai để truy cập Prefect UI, MLflow và Redpanda Console từ ngoài mạng local — hữu ích khi nhóm làm việc remote.

Startup sequence được đảm bảo bởi `depends_on + healthcheck`:
```
postgres (healthy) → iceberg-rest (healthy) → prefect-server (healthy) → prefect-worker
                  ↳ redpanda (healthy) → redpanda-console + redpanda-init
```

### **4.2 Xây dựng Ingestion Layer** {#4.2-ingestion}

#### **Batch Ingestion**

`scripts/batch_ingest_bronze.py` thực hiện batch ingestion với các bước:

1. **Đọc CSV từ R2** qua `boto3.client.get_object()` — CSV lưu tại `s3://retail-data-lake/raw/ecommerce/` và `raw/marketing/`. Đọc vào Pandas DataFrame với `read_csv(io.BytesIO(response_body), low_memory=False)`.

2. **Xử lý timestamps**: cast các timestamp columns từ string sang `datetime64[us]` — bước bắt buộc vì PyIceberg chỉ chấp nhận microsecond precision, Pandas mặc định ra nanosecond. Không cast trước → `TypeError` khi gọi `table.append()`.

3. **Thêm metadata columns**: `_ingested_at` (UTC timestamp run hiện tại), `_source_file` (tên file CSV, ví dụ `olist_orders_dataset.csv`), `_source_path` (full S3 path).

4. **Ghi vào Iceberg**: convert DataFrame sang PyArrow Table → gọi `table.append(arrow_tbl)` qua PyIceberg REST Catalog client. Table được tạo tự động nếu chưa tồn tại, với location `s3://retail-data-lake/bronze/{table_name}/`.

Hỗ trợ hai mode:
* **Append** (mặc định): thêm data vào table có sẵn — dùng cho incremental updates.
* **`--overwrite`**: drop table + recreate + append — dùng khi cần reset hoàn toàn Bronze.

#### **Streaming Ingestion**

**Producer** (`streaming/producer.py`) có kiến trúc đặc biệt: trước khi produce, nó **enrich events ngay trong bộ nhớ** bằng cách join orders với customers (thêm `customer_state`, `customer_city`) và payments (thêm `payment_value`, `payment_type`). Lý do: consumer Iceberg cần data đầy đủ để ghi có ý nghĩa vào Bronze, không cần join lại sau.

Kafka config được tối ưu cho throughput: `acks=all` (durability), `linger.ms=20` (micro-batching), `compression.type=lz4` (giảm network bandwidth).

**Consumer** (`streaming/consumer.py`) quản lý buffer theo topic với dual flush mechanism:

```
while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        # Kiểm tra time-based flush (idle quá FLUSH_INTERVAL=15s)
        for topic: if buffer not empty and age >= 15s → flush
    else:
        buffers[topic].append(json.loads(msg.value))
        if len(buffers[topic]) >= BATCH_SIZE=200:
            flush_buffer() → table.append() → consumer.commit()
```

`enable.auto.commit=False` — Kafka offset chỉ được commit **sau khi** `table.append()` thành công. Nếu `table.append()` fail, offset không commit, consumer tự replay messages từ đó khi restart — đảm bảo at-least-once delivery không mất data.

#### **Data Validation**

Trước khi ghi vào Bronze, consumer thực hiện schema alignment: so sánh DataFrame columns với Iceberg table schema, thêm columns thiếu (set null), bỏ columns thừa, reorder theo đúng thứ tự Iceberg. Sau đó dùng `schema_to_pyarrow(iceberg_schema)` để ép kiểu dữ liệu đúng trước khi `table.append()`. Messages có JSON parse error bị log warning và skip — không làm crash consumer.

### **4.3 Xây dựng Medallion Lakehouse** {#4.3-medallion}

#### **Bronze Layer**

Sau khi batch + streaming ingestion hoàn thành, Bronze Layer có 10 tables trên R2:

```
s3://retail-data-lake/bronze/
├── ecommerce_orders/       (data/*.parquet + metadata/)
├── ecommerce_order_items/
├── ecommerce_order_payments/
├── ecommerce_order_reviews/
├── ecommerce_customers/
├── ecommerce_sellers/
├── ecommerce_products/
├── ecommerce_geolocation/
├── marketing_leads/
└── marketing_deals/
```

Mỗi thư mục chứa Parquet data files và Iceberg metadata (manifest files, snapshot JSON). Toàn bộ metadata được indexed trong PostgreSQL catalog — cho phép truy vấn catalog mà không cần scan S3.

#### **Silver Layer**

PySpark Silver transform đọc Bronze tables, thực hiện cleaning và chuẩn hóa:

* **Type casting**: tất cả timestamp columns từ string → `TimestampType()` với `to_timestamp()`.
* **Null handling**: fill null cho numeric columns (0), string columns ("unknown"), drop rows có null primary key.
* **Deduplication**: `dropDuplicates(["order_id"])` theo business key.
* **Column normalization**: chuẩn hóa tên cột, bỏ columns không cần thiết.
* **Derived fields**: `estimated_delivery_days = DATEDIFF(estimated_delivery_date, purchase_timestamp)`.

Silver strategy: incremental merge với `MERGE INTO` SQL — chỉ insert/update rows mới hoặc thay đổi so với lần chạy trước.

#### **Gold Layer**

PySpark Gold transform build Star Schema từ Silver tables:

**`fct_orders`** — fact table trung tâm, partition theo `order_date_month`:
* `gross_revenue = SUM(price + freight_value)`
* `net_revenue = SUM(price)`
* `delivery_delay_days = DATEDIFF(actual_delivery_date, estimated_delivery_date)`
* `is_late_delivery = delivery_delay_days > 0`
* `avg_review_score` — pre-aggregated tại order level

**`dim_sellers`**: seller profile + cumulative performance metrics (avg review, on-time delivery rate, total revenue).

**`dim_customers`**: customer lifetime metrics (total orders, total spent, days since last order).

**`fct_funnel`**: marketing lead-to-deal conversion metrics theo channel và segment.

### **4.4 Xây dựng Query và Analytics Layer** {#4.4-query-analytics}

#### **DuckDB Query trên Gold Layer**

DuckDB được cấu hình kết nối đến R2 qua `httpfs` extension:

```python
conn = duckdb.connect(":memory:")
conn.execute(f"""
    INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint='{S3_ENDPOINT}';
    SET s3_access_key_id='{S3_ACCESS_KEY}';
    SET s3_secret_access_key='{S3_SECRET_KEY}';
    SET s3_region='{S3_REGION}';
    SET s3_url_style='path';
""")
```

DuckDB đọc Parquet files trực tiếp từ Gold tables trên R2, tận dụng Iceberg partition pruning — query có `WHERE order_date_month = '2018-01'` chỉ đọc 1/26 partitions. Columnar execution với predicate pushdown cho phép queries điển hình hoàn thành trong 1–3 giây.

#### **BI Metrics**

Ba nhóm business metrics được xây dựng trên Gold Layer:

**Revenue Analytics**:
* Doanh thu theo tháng (line chart xu hướng)
* Doanh thu theo product category (bar chart top 10)
* Doanh thu theo state của người mua (map visualization)
* Monthly growth rate (MoM %)

**Seller Performance**:
* Top 10 sellers by revenue
* Delivery SLA compliance (% đơn hàng giao đúng hạn) theo seller
* Average review score distribution

**Customer Analytics**:
* Phân bố khách hàng theo địa lý (state)
* Repeat customer rate (mua lại trong 90 ngày)
* Average order value trend

### **4.5 Xây dựng Dashboard Realtime** {#4.5-dashboard}

Dashboard Streamlit tích hợp cả hai luồng dữ liệu — historical từ Gold Layer và realtime từ Redpanda — trong cùng một giao diện.

**Cấu trúc giao diện**: Streamlit multi-page app với sidebar hiển thị KPI snapshot từ Gold Layer (tổng đơn hàng, tổng doanh thu, avg review score) và navigation giữa các trang (Historical Analytics, Realtime Monitoring, Agentic BI).

**Historical Analytics page**: DuckDB connection đọc Gold tables từ R2, render Revenue Analytics + Seller Performance + Customer Analytics charts. Hỗ trợ date range filter và drill-down theo category.

**Realtime Monitoring page**: Kafka consumer với `@st.cache_resource` để persist buffer qua nhiều Streamlit reruns:

```python
@st.cache_resource
def get_consumer_state():
    return {
        "consumer": Consumer({...}),
        "buffer": deque(maxlen=2000),
    }
```

Auto-refresh mỗi 3 giây (Streamlit `st_autorefresh`). Consumer poll với 0.5s hard deadline mỗi chu kỳ để tránh block UI thread khi topic có backlog lớn. Metrics realtime: đơn hàng mới trong 60s, cumulative orders, phân bố `order_status`.

##### *Hình 1: Kiến trúc Dashboard — hai luồng Historical (DuckDB) và Realtime (Kafka)* {#hình-1-dashboard}

---

## **CHƯƠNG 5. ĐÁNH GIÁ HỆ THỐNG** {#chương-5-đánh-giá}

### **5.1 Kết quả đạt được** {#5.1-kết-quả}

Hệ thống đáp ứng toàn bộ functional requirements đề ra:

| Yêu cầu | FR | Kết quả thực tế |
| :--- | :---: | :--- |
| Batch Ingestion | FR-01 | 10 bảng → Bronze Iceberg, ~100K đơn hàng, ~1M geolocation records |
| Streaming Ingestion | FR-02 | 3 topics, consumer ghi liên tục với dual flush mechanism |
| Data Validation | FR-03 | Schema alignment + null check trước khi append Iceberg |
| ELT Transformation | FR-04 | PySpark Silver (clean) + Gold (Star Schema, 26 partitions `fct_orders`) |
| Orchestration | FR-05 | Prefect 3 flows, run history + Artifacts trên Prefect UI |
| Dashboard | FR-06 | Streamlit historical + realtime, DuckDB query <3s |
| Containerization | FR-07 | Core stack: `docker compose up`; optional: `--profile spark/query/bi` |

#### *Bảng 6: Kết quả theo functional requirements* {#bảng-6-kết-quả}

**Cấu trúc Lakehouse trên R2 sau pipeline run:**

```
s3://retail-data-lake/
├── raw/           ← CSV files gốc (upload bằng upload_raw_to_r2.py)
├── bronze/        ← 10 Iceberg tables (PyIceberg append)
│   ├── ecommerce_orders/ (data/*.parquet + metadata/)
│   └── ...
├── silver/        ← PySpark incremental merge
│   ├── stg_orders/
│   └── ...
└── gold/          ← PySpark partition overwrite
    ├── fct_orders/ (partitioned: order_date_month=2016-09/ ... 2018-10/)
    ├── dim_sellers/
    ├── dim_customers/
    └── fct_funnel/
```

### **5.2 Đánh giá hiệu năng** {#5.2-hiệu-năng}

#### **Query Performance**

Benchmark thực hiện trên DuckDB đọc Gold Parquet files từ R2 qua HTTPS (4 CPU / 16GB RAM):

| Query | Mô tả | Thời gian |
| :--- | :--- | :---: |
| Q1 — Simple aggregate | `SUM(net_revenue)` toàn bộ `fct_orders` | ~0.8s |
| Q2 — Monthly revenue | GROUP BY `order_date_month`, 26 tháng | ~1.2s |
| Q3 — Multi-table join | `fct_orders JOIN dim_sellers JOIN dim_customers` | ~2.4s |
| Q4 — Window function | Monthly revenue YoY comparison với `LAG()` | ~1.9s |
| Q5 — Partition filter | `WHERE order_date_month BETWEEN '2018-01' AND '2018-06'` | ~0.6s |

#### *Bảng 7: DuckDB Query Performance trên Gold Layer* {#bảng-7-query}

Q5 chỉ đọc 6/26 partitions nhờ Iceberg partition pruning — nhanh hơn Q2 (không có partition filter) 2x dù cùng loại query. Thời gian bottleneck chủ yếu là network latency khi tải Parquet files từ R2 (~1–2s overhead); khi files được cached local, queries dưới 200ms.

#### **Streaming Performance**

| Metric | Kết quả |
| :--- | :--- |
| Producer throughput (mode=rate) | 2 events/s (configurable) |
| Consumer batch latency (200 msgs) | ~2–3s (bao gồm Iceberg append + R2 write) |
| End-to-end latency (produce → Bronze visible) | ~3–5s |
| Time-based flush (idle topic) | Tối đa 15s chờ rồi flush |
| Messages/phút trong 1 giờ test | ~120 events (2 events/s × 60) |

#### *Bảng 8: Streaming Pipeline Performance* {#bảng-8-streaming}

End-to-end latency ~3–5 giây đáp ứng NFR-02 (< 5s) cho near-realtime monitoring use case.

#### **Pipeline Reliability**

Kiểm tra retry mechanism bằng cách inject lỗi có kiểm soát:

* **Test 1 — R2 connection timeout**: Prefect task `ingest_csv` fail lần 1, tự retry sau 10 giây, thành công lần 2. Flow tiếp tục bình thường — không cần can thiệp.
* **Test 2 — Redpanda restart giữa chừng**: Consumer mất kết nối, confluent-kafka library tự reconnect sau ~5s, đọc tiếp từ last committed offset (offset chỉ commit sau flush thành công). Không mất messages.
* **Test 3 — Schema mismatch trong stream message**: Consumer log warning, skip message. Buffer của topic khác không bị ảnh hưởng — consumer tiếp tục chạy.
* **Test 4 — Spark job OOM**: Prefect task fail sau 1 retry, flow fail với error message rõ ràng trên Prefect UI. Gold layer từ run trước không bị ảnh hưởng (Iceberg snapshot isolation).

### **5.3 So sánh với kiến trúc truyền thống** {#5.3-so-sánh}

| Tiêu chí | Data Warehouse truyền thống | Unified Lakehouse (đề tài) |
| :--- | :--- | :--- |
| **Realtime analytics** | Batch ETL, độ trễ giờ–ngày | Streaming → Bronze trong <5s |
| **Schema thay đổi** | ALTER TABLE + migrate data | Iceberg Schema Evolution, không downtime |
| **Streaming support** | CDC/Debezium pipeline riêng | Native: Iceberg ACID concurrent write |
| **Time travel** | Không có | Snapshot-based, bất kỳ thời điểm |
| **Chi phí lưu trữ** | Database storage (~$0.10/GB/tháng) | R2 object storage (~$0.015/GB/tháng) |
| **Scale compute** | Vertical (server lớn hơn) | Horizontal (Spark mode switch qua env var) |
| **Reproducibility** | Khó (data thay đổi in-place) | Bronze immutable, rebuild bất kỳ lúc nào |
| **Operational complexity** | Thấp (một DB instance) | Cao hơn (nhiều services) |
| **Dev cost** | Thấp | Trung bình (Docker Compose ~4GB RAM) |

#### *Bảng 9: So sánh Unified Lakehouse với Data Warehouse truyền thống* {#bảng-9-so-sánh}

Lakehouse có lợi thế rõ ràng về realtime support, schema flexibility, scale path và chi phí lưu trữ. Trade-off là operational complexity cao hơn — nhiều services cần quản lý. Tuy nhiên Docker Compose với healthchecks và Prefect với retry/alerting giảm đáng kể gánh nặng vận hành.

### **5.4 Hạn chế hệ thống** {#5.4-hạn-chế}

**H-01 — Single-node deployment**: Docker Compose chạy trên một machine. Khi dataset vượt RAM machine, Spark local mode spill to disk và performance giảm. Giải pháp: kích hoạt `--profile spark` cluster mode với `SPARK_MASTER=spark://spark-master:7077`.

**H-02 — Chưa tối ưu distributed processing**: Spark cluster hiện tại cấu hình mặc định (`SPARK_WORKER_CORES=2`, `SPARK_WORKER_MEMORY=2G`). Chưa có partition tuning, broadcast join optimization hay adaptive query execution được cấu hình.

**H-03 — Chưa triển khai Data Governance**: Không có column-level masking (PII protection cho `customer_id`, địa chỉ), row-level security, hay audit log theo user. Cần OpenMetadata hoặc Apache Atlas cho production compliance (LGPD — Brazil data protection law).

**H-04 — Iceberg snapshot accumulation**: Mỗi lần ingest tạo 1 snapshot. Sau nhiều lần chạy, số snapshots tích lũy làm chậm metadata operations. Cần chạy `table.expire_snapshots(older_than_ms=...)` định kỳ — chưa được tích hợp vào Prefect flow.

**H-05 — Chưa có CI/CD cho pipeline code**: Thay đổi Spark transform code không được tự động test. Cần GitHub Actions chạy integration test với dataset nhỏ khi có PR thay đổi `spark/jobs/`.

---

## **CHƯƠNG 6. KẾT LUẬN VÀ HƯỚNG PHÁT TRIỂN** {#chương-6-kết-luận}

### **6.1 Kết luận** {#6.1-kết-luận}

Đề tài đã xây dựng thành công một nền tảng Unified Lakehouse hoàn chỉnh end-to-end cho dữ liệu thương mại điện tử Olist, đáp ứng ba bài toán doanh nghiệp cốt lõi đặt ra từ đầu:

**Bài toán 1 (dữ liệu phân tán)** được giải quyết bằng Medallion Architecture trên Iceberg — 10 bảng từ 2 domain khác nhau được chuẩn hóa qua Bronze → Silver → Gold thành một nguồn sự thật thống nhất với schema tường minh và chất lượng đảm bảo.

**Bài toán 2 (mâu thuẫn batch và realtime)** được giải quyết bằng Lambda Architecture với Iceberg ACID: batch pipeline và streaming consumer ghi đồng thời vào cùng Bronze tables mà không conflict, nhờ Optimistic Concurrency Control của Iceberg. Một nền tảng duy nhất phục vụ cả analytical queries lịch sử (DuckDB trên Gold) và realtime monitoring (Kafka consumer + Streamlit).

**Bài toán 3 (phụ thuộc nhân sự kỹ thuật)** được giải quyết bằng Prefect orchestration: pipeline chạy tự động theo schedule, tự retry khi gặp lỗi transient, toàn bộ run history và failure details hiển thị trên UI mà không cần SSH. Prefect Artifacts cung cấp summary tables sau mỗi run để monitor nhanh mà không cần mở logs.

Điểm kỹ thuật nổi bật nhất là **PostgreSQL JDBC Catalog** cho Iceberg — không phải in-memory catalog thường thấy trong demo. Catalog persistent đảm bảo table registration, schema history và snapshot metadata không mất khi restart, là nền tảng cho production deployment thực sự.

### **6.2 Hướng phát triển** {#6.2-hướng-phát-triển}

**Distributed deployment**: Triển khai lên Kubernetes với Helm charts cho từng component. Spark cluster scale horizontal theo workload với Kubernetes operator. Prefect workers scale theo queue depth.

**Query federation với Trino**: Tận dụng `--profile query` đã có sẵn trong Docker Compose — join Iceberg Gold tables với PostgreSQL external metadata, CSV reference data trong một SQL query duy nhất mà không cần ETL.

**Semantic layer**: Xây dựng metric definitions (revenue, churn rate, delivery SLA) trong một semantic layer (MetricFlow, LookML) để đảm bảo consistency giữa dashboard, Agentic BI và ad-hoc queries — tránh tình trạng mỗi query tính "doanh thu" theo cách khác nhau.

**Cloud-native architecture**: Thay Docker Compose bằng managed services: Cloudflare R2 (đã dùng) + Confluent Cloud (Kafka) + Databricks (Spark + Delta Lake) hoặc Snowflake Iceberg + dbt Cloud. Giữ nguyên business logic trong Spark/dbt models, chỉ thay infrastructure layer.

**Data Governance**: Tích hợp OpenMetadata để catalog tất cả tables, track lineage từ Bronze đến Gold, tự động phát hiện PII columns và apply masking policy — đáp ứng LGPD (Brazil) và GDPR compliance.

---

# **PHẦN C: ĐÓNG GÓP VÀ CÔNG VIỆC THỰC HIỆN CÁ NHÂN** {#phần-c}

---

## **CHƯƠNG 1: TỔNG QUAN VAI TRÒ CÁ NHÂN** {#c-chương-1-vai-trò}

### **1.1 Phân chia công việc trong nhóm** {#c-1.1-phân-chia}

Dự án được chia thành hai mảng công việc tách biệt về trách nhiệm: **Data Platform Layer** và **Intelligence Layer**. Phần đóng góp cá nhân của tác giả tập trung toàn bộ vào **Data Platform Layer** — toàn bộ hạ tầng nền tảng của Lakehouse: khởi tạo Iceberg catalog và tables, pipeline ingestion batch và streaming, data validation, PySpark transformation pipeline Silver/Gold, Prefect orchestration và Docker Compose infrastructure.

Thành viên nhóm Hồ Ngọc Chương đảm nhận **Intelligence Layer** — Agentic BI (natural language → SQL → chart) và Streamlit dashboard, được xây dựng trên nền tảng Gold tables đã có sẵn.

Điểm phân chia này có ý nghĩa kiến trúc: Data Platform Layer là điều kiện tiên quyết để Intelligence Layer tồn tại. Đồng thời, thiết kế tách biệt chứng minh tính modular của Lakehouse — Intelligence Layer có thể bổ sung mà không cần sửa bất kỳ code nào ở Platform Layer bên dưới.

### **1.2 Phạm vi công việc cá nhân** {#c-1.2-phạm-vi}

| Module | File chính | Mô tả |
| :--- | :--- | :--- |
| Batch Ingestion | `scripts/batch_ingest_bronze.py` | CSV từ R2 raw/ → Bronze Iceberg (10 bảng) |
| Streaming Producer | `streaming/producer.py` | Replay/rate events → 3 Redpanda topics |
| Streaming Consumer | `streaming/consumer.py` | Redpanda → Bronze Iceberg (dual flush) |
| PySpark Silver | `spark/jobs/silver_transform.py` | Bronze → Silver (clean, type, dedup) |
| PySpark Gold | `spark/jobs/gold_transform.py` | Silver → Gold (Star Schema, partition) |
| Prefect Orchestration | `prefect/flows/` | 4 flows với retry, Artifacts, scheduling |
| Infrastructure | `docker-compose.yml` | 8 core services + 4 optional profiles |

#### *Bảng C-1: Phạm vi công việc cá nhân* {#bảng-c1}

### **1.3 Bài toán kỹ thuật cốt lõi** {#c-1.3-bài-toán}

Ba thách thức kỹ thuật then chốt đặt ra trong Data Platform Layer:

**Thách thức 1 — Ghi đồng thời không conflict**: Batch pipeline và streaming consumer phải ghi vào cùng Bronze table `ecommerce_orders` mà không tạo ra partial writes hay inconsistent reads. Giải pháp: Iceberg Optimistic Concurrency Control — mỗi writer đọc current snapshot, atomic commit, retry nếu snapshot đã thay đổi do concurrent writer.

**Thách thức 2 — Schema thay đổi không phá vỡ downstream**: Khi upstream thêm field mới vào event payload, Bronze phải nhận được field đó ngay mà không cần dừng pipeline. Iceberg Schema Evolution + consumer schema alignment (thêm cột thiếu = null) giải quyết điều này.

**Thách thức 3 — Pipeline hoàn toàn tự động**: Không có can thiệp thủ công — pipeline chạy theo schedule, tự retry khi lỗi transient, tự dừng nếu fail sau tất cả retries và hiển thị failure reason trên UI.

---

## **CHƯƠNG 2: XÂY DỰNG BRONZE LAYER** {#c-chương-2-bronze}

### **2.1 Batch Ingestion — CSV từ R2 vào Iceberg** {#c-2.1-batch}

`batch_ingest_bronze.py` giải quyết một vấn đề kỹ thuật thực tế quan trọng khi làm việc với PyIceberg: **timestamp precision mismatch**. `pandas.read_csv()` parse timestamps thành `datetime64[ns]` (nanosecond), nhưng PyIceberg 0.7+ chỉ chấp nhận `datetime64[us]` (microsecond). Nếu không có bước convert, `pa.Table.from_pandas()` tạo ra Arrow schema với `timestamp[ns]` không khớp với Iceberg schema `timestamp[us]`, dẫn đến `ValueError` khi gọi `table.append()`.

Giải pháp được implement:

```python
for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
    df[col] = df[col].astype("datetime64[us]")
```

Đây không phải detail nhỏ — đây là lỗi phổ biến khi dùng PyIceberg với Pandas mà tài liệu chính thức không làm rõ. Phát hiện và fix lỗi này là kết quả của quá trình debug thực tế.

Cấu trúc ingest theo DATASETS list với 3 fields mỗi entry: `(r2_key, table_name, timestamp_columns)` — tách biệt config khỏi logic, dễ thêm bảng mới mà không sửa hàm `ingest_table()`.

### **2.2 Streaming Producer — Hai chế độ vận hành** {#c-2.2-producer}

Producer được thiết kế với hai chế độ để phục vụ hai use case khác nhau:

**Mode `rate`** (mặc định): gửi `RATE=2` events/giây, lặp vô hạn qua dataset, gán timestamp = NOW. Phù hợp cho demo dashboard realtime — mỗi event mang timestamp hiện tại nên chart realtime hiển thị "đơn hàng mới đang đến" một cách trực quan.

**Mode `replay`**: sort events theo `order_purchase_timestamp` gốc, nén thời gian theo `SPEED_FACTOR=86400`. Phù hợp cho kiểm tra temporal ordering và test pipeline xử lý out-of-order events.

**Event enrichment trong producer**: trước khi publish lên Redpanda, producer join orders với customers (thêm `customer_state`, `customer_city`) và với payments (aggregate `payment_value`, lấy `payment_type` của `payment_sequential=1`). Việc enrich tại producer thay vì tại consumer giúp Bronze `ecommerce_orders` stream có đủ context cho analytics mà không cần join ở downstream.

### **2.3 Streaming Consumer — Dual Flush và Schema Alignment** {#c-2.3-consumer}

Consumer có hai cơ chế đặc biệt cần giải thích:

**Dual flush mechanism**: flush xảy ra khi (1) buffer của một topic đạt `BATCH_SIZE=200` messages, HOẶC (2) buffer có data và đã idle quá `FLUSH_INTERVAL=15` giây. Flush thứ hai xử lý trường hợp traffic thấp — nếu topic chỉ có 5 messages/giờ, không có dual flush thì data sẽ stuck trong buffer mãi không ghi được vào Bronze.

**Schema alignment trước khi append**:

```python
existing_cols = [f.name for f in iceberg_tbl.schema().fields]
for col in existing_cols:
    if col not in df.columns:
        df[col] = None          # thêm cột thiếu
df = df[existing_cols]          # reorder + bỏ cột thừa
arrow_schema = schema_to_pyarrow(iceberg_schema)
arrow_tbl = pa.Table.from_pandas(df, schema=arrow_schema)
```

Khi Iceberg table có schema cũ hơn message payload (ví dụ: producer thêm field mới chưa kịp update Bronze schema), consumer thêm columns thiếu với giá trị null và bỏ columns thừa từ payload — đảm bảo append không bao giờ fail vì schema mismatch.

---

## **CHƯƠNG 3: TRANSFORMATION PIPELINE VÀ ORCHESTRATION** {#c-chương-3-transform}

### **3.1 PySpark Transformation** {#c-3.1-pyspark}

Spark transformation pipeline được thiết kế với một tính năng quan trọng: **Spark mode hoàn toàn điều khiển bởi `SPARK_MASTER` environment variable** trong `.env`:

```bash
SPARK_MASTER=local[2]                # dev: local, 2 threads (mặc định)
SPARK_MASTER=spark://spark-master:7077  # prod: cluster (cần --profile spark)
```

Khi `SPARK_MASTER=local[2]`, PySpark chạy in-process bên trong `prefect-worker` container — không cần dựng cluster riêng, phù hợp cho dataset 200MB. Khi cần scale, chỉ đổi 1 biến env và start Spark cluster profile — không có thay đổi nào trong transform code hay Prefect flow.

`get_spark()` function trong mỗi job file build SparkSession với Iceberg Spark Catalog:

```python
spark = SparkSession.builder \
    .master(os.environ.get("SPARK_MASTER", "local[2]")) \
    .config("spark.sql.catalog.olist", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.olist.type", "rest") \
    .config("spark.sql.catalog.olist.uri", ICEBERG_REST_URI) \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .getOrCreate()
```

### **3.2 Prefect Flows và Artifacts** {#c-3.2-prefect}

Bốn flows được xây dựng với trách nhiệm tách biệt:

**`bronze-batch-ingestion`** (`bronze_ingestion.py`): Task `ensure_namespace` (retries=2, 5s delay) tạo Bronze namespace nếu chưa có — cần retry vì `iceberg-rest` container có thể chưa fully ready khi Prefect worker start lần đầu. Task `ingest_csv` (retries=1, 10s delay) thực hiện ingest từng bảng. Sau khi tất cả 10 bảng xong, tạo **Table Artifact** hiển thị summary trực tiếp trên Prefect UI:

```python
create_table_artifact(
    key="bronze-ingestion-summary",
    table=[{"table": t, "rows": r, "source": s} for t,r,s in results],
)
```

**`silver-transform`** (`spark_transforms.py`): Wrap `silver_transform.py` PySpark job trong một Prefect task (retries=1, 30s delay). Sau khi chạy xong, lấy danh sách tables trong `olist.silver` catalog để tạo **Markdown Artifact** báo cáo kết quả.

**`gold-transform`** (`spark_transforms.py`): Tương tự silver, wrap `gold_transform.py`. Artifact liệt kê tables trong `olist.gold`.

**`full-pipeline`** (`full_pipeline.py`): Entry point gọi 3 flows theo thứ tự tuần tự (Bronze → Silver → Gold). Hai operational parameters:
* `skip_bronze=True`: bỏ qua Bronze ingestion, chỉ chạy lại transforms — tiết kiệm thời gian khi chỉ cần fix Spark logic.
* `full_refresh=True`: pass xuống Silver/Gold để force full overwrite thay vì incremental — dùng sau schema change lớn.

**Retry strategy tổng hợp**:

| Task | retries | delay | Lý do |
| :--- | :---: | :---: | :--- |
| `ensure_namespace` | 2 | 5s | `iceberg-rest` chưa ready khi worker start |
| `ingest_csv` | 1 | 10s | Network timeout R2 write |
| `run_dbt` (dbt_transforms) | 1 | 30s | DuckDB lock tạm thời |
| `run_silver_task` | 1 | 30s | Spark session startup failure |
| `run_gold_task` | 1 | 30s | Spark session startup failure |
| `run_dbt_test` | 0 | — | Test fail là signal thực, không retry |

#### *Bảng C-2: Retry configuration theo Prefect task* {#bảng-c2-retry}

---

## **CHƯƠNG 4: ĐÁNH GIÁ VÀ BÀI HỌC RÚT RA** {#c-chương-4-đánh-giá}

### **4.1 Kết quả đạt được** {#c-4.1-kết-quả}

Data Platform Layer đáp ứng toàn bộ mục tiêu kỹ thuật đặt ra, được xác nhận qua các test cases cụ thể:

**Thách thức 1 (concurrent write)**: Khởi động `batch_ingest_bronze.py` và `streaming/consumer.py` đồng thời trỏ vào cùng Bronze table `ecommerce_orders`. Cả hai hoàn thành thành công, không có corrupt snapshot hay partial write. Bronze table chứa records từ cả hai nguồn, phân biệt được qua `_source_file` column.

**Thách thức 2 (Schema Evolution)**: Thêm column mới vào Bronze schema qua PyIceberg `UpdateSchema` API. Streaming consumer tự align — thêm null cho column thiếu trong existing messages, ghi được vào table mới mà không cần restart. Spark Silver transform đọc được column mới từ Bronze, downstream Gold models không bị ảnh hưởng.

**Thách thức 3 (pipeline tự động)**: Full pipeline flow chạy từ `docker compose up` đến Gold tables hoàn chỉnh với `full_pipeline_flow()` call. Inject network timeout khi upload R2 → Prefect task retry sau 10s → thành công lần 2 → flow tiếp tục. Toàn bộ run visible trên Prefect Server UI tại `localhost:4200`.

### **4.2 Các quyết định thiết kế có tác động lớn nhất** {#c-4.2-quyết-định}

**Quyết định 1 — PostgreSQL JDBC Catalog thay vì in-memory**

Hầu hết tutorial Iceberg REST dùng in-memory catalog cho đơn giản. Quyết định dùng JDBC backend với PostgreSQL có nghĩa là mọi table registration và schema history được lưu persistent — catalog không mất khi restart `iceberg-rest` container. Đây là điều kiện bắt buộc cho production deployment thực sự, không chỉ là demo.

**Quyết định 2 — Dual flush (batch + time-based)**

Flush chỉ theo batch size sẽ làm data stuck trong buffer khi traffic thấp (ví dụ: cuối tuần ít đơn hàng). Time-based flush sau 15 giây đảm bảo không có data nào bị giữ lâu quá `FLUSH_INTERVAL` giây bất kể traffic thấp đến đâu. Đây là pattern phổ biến trong production Kafka consumers nhưng thường bị bỏ qua trong implementations đơn giản.

**Quyết định 3 — `SPARK_MASTER` env var cho Spark mode switching**

Thay vì hardcode Spark master URL trong code hay có 2 bộ code khác nhau cho dev/prod, dùng environment variable để switch mode. Điều này cho phép cùng một `full_pipeline_flow()` chạy được ở cả `local[2]` (dev laptop) và `spark://cluster:7077` (production) mà không cần sửa bất kỳ dòng code nào — chỉ thay đổi `.env`.

### **4.3 Hạn chế và bài học rút ra** {#c-4.3-hạn-chế}

**Hạn chế 1 — Iceberg snapshot accumulation**: Mỗi `table.append()` tạo 1 snapshot + manifest files + data files trên R2. Sau 100 lần ingest, Bronze table có 100 snapshots. `table.scan()` phải đọc qua snapshot chain để tìm current state — chậm dần theo thời gian. Cần tích hợp `table.expire_snapshots(older_than_ms=7*24*3600*1000)` vào Prefect flow, chạy sau mỗi successful pipeline run.

**Hạn chế 2 — Missing dead letter queue cho streaming**: Messages có JSON parse error hiện tại bị skip và log warning — data bị mất hoàn toàn. Trong production, cần publish malformed messages sang topic `olist.orders.dlq` (dead letter queue) để review và reprocess thủ công sau.

**Hạn chế 3 — Prefect worker volume mount làm phức tạp local dev**: `prefect-worker` container dùng `volumes` để mount code từ local filesystem vào container. Khi đổi code, không cần rebuild image nhưng phải restart container. Tốt hơn là dùng Prefect `git storage` hoặc S3 block để worker tự pull code khi run flow.

**Bài học rút ra**:
* Timestamp precision (`ns` vs `us`) là loại lỗi không có error message rõ ràng — chỉ phát hiện khi runtime `ValueError`. Luôn check PyIceberg version compatibility khi làm việc với timestamp columns.
* PostgreSQL healthcheck trong Docker Compose không đủ để đảm bảo database sẵn sàng nhận connections — cần thêm delay hoặc retry ở application level khi `iceberg-rest` lần đầu connect.
* Prefect `create_table_artifact` và `create_markdown_artifact` cực kỳ hữu ích cho monitoring nhanh — team không cần mở terminal để biết pipeline vừa chạy ingest được bao nhiêu rows.

---

## **KẾT LUẬN PHẦN C** {#c-kết-luận}

Phần đóng góp cá nhân đã xây dựng hoàn chỉnh **Data Platform Layer** — từ batch ingestion, streaming pipeline với dual flush mechanism, PySpark transformation Medallion, đến Prefect orchestration với Artifacts và optional Spark cluster support. Nền tảng này phục vụ trực tiếp cho Intelligence Layer (Agentic BI + Dashboard) của thành viên nhóm và minh họa một Lakehouse production-grade hoạt động end-to-end trong Docker Compose.

Ba kỹ thuật quan trọng nhất được đúc kết từ dự án: (1) PostgreSQL JDBC Catalog là điều kiện bắt buộc để Iceberg catalog hoạt động persistent trong containerized environment; (2) dual flush (batch + time-based) là pattern thiết yếu để streaming consumer không để data stuck trong buffer; (3) environment-variable-driven Spark mode là cách đúng đắn để cùng một codebase chạy được ở cả local dev và production cluster mà không cần điều kiện rẽ nhánh trong code.
