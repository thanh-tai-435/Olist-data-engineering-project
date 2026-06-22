# Script Người 2 — Ingestion + Redpanda + Prefect
**Olist Data Lakehouse Platform** · Thời gian: **3:00 – 6:00** · 3 phút · 2 demo live

---

## Phân bố thời gian

| Giai đoạn | Thời điểm | Nội dung | Loại |
|-----------|-----------|----------|------|
| 0 | 3:00 – 3:10 | Tiếp nhận mic từ Người 1, mở đầu | Lời nói |
| 1 | 3:10 – 3:40 | Batch Ingestion — CSV → PyIceberg Bronze | Lời nói + code |
| 2 | 3:40 – 4:30 | Streaming — Redpanda Console + producer/consumer | **DEMO live** |
| 3 | 4:30 – 5:20 | Prefect UI — flow run, task graph, quality gate | **DEMO live** |
| 4 | 5:20 – 5:50 | Tổng kết + chuyển tiếp sang Người 3 | Lời nói |

---

## ⚠️ Chuẩn bị trước khi lên

- [ ] Tab **Redpanda Console** mở sẵn tại `http://localhost:8080` — topic `olist.orders` đã có message  
- [ ] Tab **Prefect UI** mở sẵn tại `http://localhost:4200` — có ít nhất 1 flow run **Completed**  
- [ ] Producer đang chạy: `docker exec olist-producer python streaming/producer.py`  
- [ ] Consumer đang chạy: `docker exec olist-consumer python streaming/consumer.py`  
- [ ] Biết sẵn câu trả lời: *"Tại sao dùng Redpanda mà không dùng Kafka thật?"*

---

## Giai đoạn 0 — Tiếp nhận & Mở đầu (3:00 – 3:10)

> 💬 **Lời nói:**

*"Cảm ơn bạn. Vậy là chúng ta đã hiểu cách system nhìn tổng thể. Bây giờ phần của mình sẽ đi sâu vào **hai đầu vào của hệ thống** — dữ liệu vào Bronze layer bằng cách nào, và ai điều phối toàn bộ flow."*

---

## Giai đoạn 1 — Batch Ingestion (3:10 – 3:40)

> 💬 **Lời nói:**

*"Luồng đầu tiên — **Batch Ingestion**. 8 file CSV gốc của Olist, tổng khoảng 200 MB, được đọc bằng Pandas và ghi vào **Bronze layer trên Cloudflare R2** thông qua PyIceberg.*

*Thiết kế quan trọng nhất của Bronze: **append-only, immutable**. Tức là không bao giờ UPDATE hay DELETE. Nếu source data thay đổi, chúng ta append snapshot mới. Lịch sử luôn được giữ nguyên — đây là nền tảng của time travel sau này.*

*Kết quả: 10 Iceberg table trên R2, mỗi table có `data/` chứa Parquet và `metadata/` chứa snapshots, manifests — đây là cấu trúc của Iceberg Table Format."*

> 📋 **Code tham chiếu** (`prefect/flows/bronze_ingestion.py`):

```python
@task(retries=3, retry_delay_seconds=30)
def ingest_dataset(dataset: str, catalog) -> int:
    table = catalog.load_table(f"bronze.ecom.{dataset}")
    df = pd.read_csv(f"data/olist_{dataset}_dataset.csv")
    df = df.where(pd.notna(df), None)   # NaN -> None (Iceberg-safe)

    pa_table = pa.Table.from_pandas(df, schema=table.schema().as_arrow())
    table.append(pa_table)              # append-only, ACID
    return len(df)
```

> 🔑 **Điểm kỹ thuật:**
> - `@task(retries=3)` — nếu S3/R2 timeout, tự retry, không cần can thiệp thủ công
> - `table.append()` — PyIceberg API, **không bao giờ** dùng raw Parquet write
> - `df.where(pd.notna(df), None)` — chuyển NaN sang None, Iceberg không hiểu NaN của pandas

---

## Giai đoạn 2 — Redpanda Streaming (3:40 – 4:30)

### Bước 1 — Mở Redpanda Console (3:40 – 3:55)

> 🖥️ **Thao tác màn hình:**
> 1. Switch sang tab **Redpanda Console** (`localhost:8080`)
> 2. Click menu bên trái: **Topics** → click `olist.orders`
> 3. Thấy danh sách messages — click một message bất kỳ để xem payload JSON
> 4. Expand JSON ra để thấy `order_id`, `customer_id`, `timestamp`

> 💬 **Lời nói:**

*"Đây là **Redpanda Console** — giao diện giám sát broker. Topic `olist.orders` đang chạy real-time. Nhìn vào một message bất kỳ — payload JSON có `order_id`, `customer_id`, `timestamp`.*

*Message key là `order_id` — điều này đảm bảo tất cả sự kiện của cùng một đơn hàng rơi vào cùng một partition — **ordering guarantee** cho mỗi đơn hàng."*

---

### Bước 2 — Giải thích kiến trúc Streaming (3:55 – 4:15)

> 💬 **Lời nói:**

*"Redpanda chạy trong một container duy nhất — không cần Zookeeper, không cần JVM. RAM tiêu thụ dưới **500 MB**, so với Kafka thật cần **2 GB**. API hoàn toàn Kafka-compatible — code Python dùng `confluent-kafka` không cần thay đổi một dòng.*

***Producer** (`streaming/producer.py`) đọc CSV gốc, sort theo `order_purchase_timestamp`, rồi replay với **speed factor 86400** — tức là 1 ngày lịch sử bằng 1 giây real-time. Điều này giúp demo streaming mà không cần chờ thật.*

***Consumer** (`streaming/consumer.py`) thuộc consumer group `iceberg-bronze-writer` — nhận message và ghi vào cùng Bronze Iceberg table với `table.append()` — cùng API với batch, không conflict nhờ ACID của Iceberg."*

> 📋 **Code Producer** (`streaming/producer.py`):

```python
SPEED_FACTOR = 86_400   # 1 day of history = 1 second realtime

df = pd.read_csv("data/olist_orders_dataset.csv",
                 parse_dates=["order_purchase_timestamp"])

prev_ts = None
for ev in df.sort_values("order_purchase_timestamp").itertuples():
    if prev_ts is not None:
        gap = (ev.order_purchase_timestamp - prev_ts).total_seconds() / SPEED_FACTOR
        if gap > 0:
            time.sleep(gap)
    producer.produce(
        topic="olist.orders",
        key=ev.order_id,                      # partition ordering
        value=json.dumps({
            "event_type":  "order_created",
            "order_id":    ev.order_id,
            "customer_id": ev.customer_id,
            "timestamp":   str(ev.order_purchase_timestamp),
        }),
    )
    prev_ts = ev.order_purchase_timestamp
producer.flush()
```

---

### Bước 3 — Consumer micro-batch (4:15 – 4:30)

> 📋 **Code Consumer** (`streaming/consumer.py`):

```python
BATCH_SIZE = 50
consumer = Consumer({
    "bootstrap.servers": "redpanda:9092",
    "group.id": "iceberg-bronze-writer",    # consumer group
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["olist.orders"])

buffer = []
while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    buffer.append(json.loads(msg.value()))

    if len(buffer) >= BATCH_SIZE:
        pa_batch = pa.Table.from_pylist(buffer, schema=BRONZE_SCHEMA)
        iceberg_table.append(pa_batch)   # ACID append to Bronze
        consumer.commit()                 # commit offset AFTER flush
        buffer.clear()
```

> 💬 **Lời nói:**

*"Consumer chạy liên tục. Mỗi lần nhận được 50 message, nó flush một lần vào Iceberg — mô hình **micro-batch** giúp giảm overhead ghi và tăng throughput. Nếu consumer restart giữa chừng, Kafka offset đã commit — không mất message, không duplicate."*

> 📌 **Bảng topics:**

| Topic | Key | Consumer Group |
|-------|-----|----------------|
| `olist.orders` | `order_id` | `iceberg-bronze-writer` |
| `olist.reviews` | `review_id` | `iceberg-bronze-writer` |
| `olist.leads` | `mql_id` | `iceberg-bronze-writer` |
| `olist.deals` | `mql_id` | `iceberg-bronze-writer` |

---

## Giai đoạn 3 — Prefect Orchestration (4:30 – 5:20)

### Bước 1 — Mở Prefect UI, chọn flow run (4:30 – 4:45)

> 🖥️ **Thao tác màn hình:**
> 1. Switch sang tab **Prefect UI** (`localhost:4200`)
> 2. Click **Flow Runs** trên sidebar trái
> 3. Tìm run tên `full-pipeline` có status ✅ **Completed**
> 4. Click vào run đó để mở detail view — xem **Task Graph**

> 💬 **Lời nói:**

*"Đây là **Prefect UI** — nơi giám sát toàn bộ pipeline. Mỗi hình tròn trên **Task Graph** này là một task Python. Màu xanh lá là Completed, màu đỏ là Failed.*

*Thấy rõ thứ tự: **Bronze Ingestion** → **Soda Check Bronze** → **Silver Transform** → **Soda Check Silver** → **Gold Transform** — toàn bộ luồng được định nghĩa bằng code Python, không cần YAML, không cần công cụ riêng."*

---

### Bước 2 — Click vào Soda Check task (4:45 – 5:05)

> 🖥️ **Thao tác màn hình:**
> 1. Trên Task Graph, click vào task `soda_check_bronze`
> 2. Click tab **Logs** để xem output của Soda Core
> 3. Đọc vài dòng: *"Pass: not_null check on order_id..."*
> 4. Quay lại Task Graph, chỉ vào mũi tên giữa Soda và Silver

> 💬 **Lời nói:**

*"Task **Soda Check** là **quality gate** giữa mỗi layer. Soda Core kiểm tra:*

*Thứ nhất là **null rate** — nếu `order_id` có null, fail ngay.*  
*Thứ hai là **row count** — phải có ít nhất 90 nghìn dòng.*  
*Thứ ba là **referential integrity** — mỗi order phải tồn tại trong bảng customers.*

*Nếu bất kỳ check nào fail, Prefect **dừng flow lại ngay tại đây**, không tiếp tục lên Silver. Và gửi webhook notification — thực tế có thể dùng Slack hay email. Đây là **data quality as code**, không cần kiểm tra thủ công."*

---

### Bước 3 — Chỉ Deployments (5:05 – 5:20)

> 🖥️ **Thao tác màn hình:**
> 1. Click **Deployments** trên sidebar
> 2. Chỉ vào schedule `Daily 02:00 UTC` của `full-pipeline`
> 3. Chỉ nhanh 4 flow có trong hệ thống

> 💬 **Lời nói:**

*"Trong **Deployments**, mỗi flow đã được đăng ký với schedule. `full-pipeline` chạy 2 giờ sáng mỗi ngày. `ml-training` chạy hàng tuần để retrain model. `sentiment-flow` chạy 3 giờ sáng score các review mới.*

*Quan trọng: Prefect Worker chạy trong container `prefect-worker`, có đủ Python + Spark. Nếu worker bị restart, các scheduled flow vẫn chạy bình thường — Prefect Server giữ state riêng."*

> 📋 **Code Flow** (`prefect/flows/full_pipeline.py`):

```python
from prefect import flow, task

@task(retries=3, retry_delay_seconds=60)
def bronze_ingestion_task():
    bronze_batch_flow()

@task(retries=2)
def soda_check_task(layer: str):
    result = run_checks(layer)
    if not result:
        raise ValueError(f"Soda check FAILED for layer={layer}")

@task(retries=2, retry_delay_seconds=30)
def spark_transform_task(job: str):
    subprocess.run(["spark-submit", f"spark/jobs/{job}.py"], check=True)

@flow(name="full-pipeline")
def full_pipeline():
    bronze_ingestion_task()           # Step 1
    soda_check_task("bronze")         # Step 2 — quality gate, blocks if fail
    spark_transform_task("silver_transform")  # Step 3
    soda_check_task("silver")         # Step 4 — quality gate
    spark_transform_task("gold_transform")    # Step 5
```

> 📌 **Bảng flows:**

| Flow | Mô tả | Schedule |
|------|-------|----------|
| `full-pipeline` | Bronze → Silver → Gold + quality gates | Daily 02:00 UTC |
| `ml-training` | Retrain 3 XGBoost models | Weekly |
| `sentiment-flow` | Score reviews mới bằng BERTimbau | Daily 03:00 UTC |
| `quality-checks` | Standalone Soda check | On-demand |

---

## Giai đoạn 4 — Tổng kết & Chuyển tiếp (5:20 – 6:00)

> 💬 **Lời nói:**

*"Tóm lại phần Ingestion: hệ thống có **hai luồng vào song song**: luồng batch qua CSV – PyIceberg, và luồng streaming qua Redpanda – Consumer. Cả hai ghi vào cùng Bronze Iceberg table, không conflict, vì Iceberg đảm bảo ACID.*

*Prefect điều phối toàn bộ luồng với quality gate Soda Core ngăn data xấu tiếp tục lên Silver. Mỗi task có retry tự động.*

*Đáng chú ý là toàn bộ phần này **không có file YAML cấu hình** — mọi thứ là Python thuần, rất dễ test và debug."*

> ➡️ **Câu chuyển tiếp:**

*"Dữ liệu đã vào Bronze. Bước tiếp theo là **transform** từ Bronze lên Silver rồi Gold — và **lineage** để biết dữ liệu đã đi qua những bước nào. Mình xin nhường lại lời cho bạn [tên Người 3]."*

---

## Q&A Chuẩn bị — 7 câu hay gặp

### Q1: Tại sao dùng Redpanda mà không dùng Kafka thật?

> *"Redpanda Kafka-compatible hoàn toàn — code Python không cần thay đổi. Điểm khác biệt: Redpanda chạy trong 1 container, không cần Zookeeper, không cần JVM — tiêu thụ dưới 500 MB RAM so với Kafka cần 2 GB+. Với quy mô đồ án sinh viên, Redpanda giúp deploy đơn giản hơn rất nhiều."*

### Q2: Tại sao Bronze append-only? Nếu source data sai thì sao?

> *"Bronze là **immutable raw data** — giữ lại đúng những gì hệ thống ngoài gửi vào. Nếu source sai, chúng ta append bản sửa lên Bronze, rồi Silver MERGE INTO sẽ dùng bản mới nhất. Bronze giữ toàn bộ lịch sử để audit."*

### Q3: Nếu Consumer chết giữa chừng thì message có mất không?

> *"Không. Consumer chỉ `commit offset` sau khi đã `flush` thành công vào Iceberg. Nếu crash trước commit, lần restart tiếp theo consumer sẽ đọc lại từ offset cũ — **at-least-once delivery**. Iceberg ACID loại duplicate nếu cần."*

### Q4: Prefect khác gì Airflow?

> *"Prefect dùng Python decorator `@flow` / `@task` — viết như code Python bình thường, không cần DAG class hay YAML cấu hình. Prefect Cloud có free tier nên không cần tự host UI. Airflow nặng hơn, cần server riêng, cú pháp DAG phức tạp hơn."*

### Q5: Quality gate Soda Core kiểm tra cụ thể những gì?

> *"Ba loại check chính: (1) **Completeness** — not_null trên các khóa chính. (2) **Volume** — row_count phải >= 90.000, phát hiện mất data. (3) **Referential integrity** — mỗi `order_id` trong `order_items` phải có trong `orders`."*

### Q6: Tại sao không dùng dbt thay vì PySpark cho Silver/Gold?

> *"dbt đã bị loại khỏi dự án. Lý do: dbt cần adapter riêng cho Iceberg, thêm toolchain phức tạp. PySpark đọc/ghi Iceberg native qua `spark-iceberg` extension, cùng engine xử lý cả small và TB-scale."*

### Q7: Hệ thống này scale lên production thật được không?

> *"Được. Thay Cloudflare R2 bằng AWS S3, thêm Spark cluster thay cho `local[2]`, thêm Kafka cluster thay Redpanda. Các abstraction lớp trên (PyIceberg, confluent-kafka, PySpark) giữ nguyên — không thay đổi code."*

---

## Cheat Sheet — Tóm tắt 1 trang

### Batch Ingestion
- 8 CSV → Pandas → PyIceberg `table.append()` — ACID
- `@task(retries=3)` — tự retry nếu S3 timeout
- Bronze: append-only, immutable, 10 Iceberg tables trên R2

### Redpanda Streaming
- 1 container, không Zookeeper, không JVM
- Kafka-compatible API — code Python giữ nguyên
- < 500 MB RAM vs Kafka 2 GB
- Speed factor 86400 (1 ngày = 1 giây) để demo
- Key = `order_id` — partition ordering guarantee
- Micro-batch flush mỗi 50 messages

### Prefect Orchestration
- `@flow` + `@task` — Python thuần, không YAML
- `wait_for` = dependency graph giữa các task
- `retries=3` mỗi task — tự xử lý transient error
- Schedule Daily 02:00 UTC, Weekly, On-demand
- Prefect Cloud free tier — không cần tự host UI

### Soda Core Quality Gate
- Check sau mỗi layer Bronze, Silver
- Null check + row count + referential integrity
- Fail → PAUSE flow ngay, không lên layer tiếp
- Webhook notification khi fail (Slack, email)

### Pipeline tổng quát

```
CSV ──► Redpanda ──► Bronze Iceberg ──[Soda?]──► Silver PySpark ──[Soda?]──► Gold PySpark ──► BI / ML
          (stream)    (append-only)    (gate)     (MERGE INTO)    (gate)     (partitioned)
```

---

*Script Người 2 · Olist Data Lakehouse · Files liên quan: `streaming/producer.py` · `streaming/consumer.py` · `prefect/flows/full_pipeline.py` · `quality/soda_runner.py`*
