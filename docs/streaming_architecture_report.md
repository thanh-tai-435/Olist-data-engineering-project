# Kiến Trúc Xử Lý Streaming: Giả Lập Dữ Liệu Thời Gian Thực với Redpanda và Apache Iceberg

---

## 1. Tổng quan Kiến trúc Lambda

Hệ thống đồ án áp dụng **kiến trúc Lambda** (*Lambda Architecture*), trong đó hai luồng xử lý song song tồn tại và bổ trợ cho nhau (Marz & Warren, 2015, tr. 12–18):

- **Batch layer**: Ingest toàn bộ dataset Olist từ CSV lên Bronze Iceberg thông qua PyIceberg.
- **Speed layer (streaming)**: Giả lập luồng sự kiện theo thời gian thực, đưa dữ liệu vào cùng Bronze Iceberg thông qua Redpanda và consumer micro-batch.

Cả hai luồng đều ghi vào cùng một lớp lưu trữ (Iceberg trên Cloudflare R2), đảm bảo tính nhất quán dữ liệu. Kleppmann (2017, tr. 498–502) lập luận rằng điểm mạnh của kiến trúc Lambda là khả năng tái xử lý (*reprocessing*) toàn bộ lịch sử từ batch layer trong khi speed layer cung cấp kết quả gần thời gian thực (*near-real-time*) với độ trễ thấp.

```
CSV Dataset (Olist)
       │
       ├──── Batch Ingest ──────────────────────────────► Bronze Iceberg (R2)
       │     (PyIceberg, append-only)                           │
       │                                                        │
       └──── Streaming Simulation ──► Redpanda ──► Consumer ──►┘
             (Producer – rate/replay)   (Kafka)   (micro-batch)
                                                        │
                                             Real-Time Dashboard
                                             (Streamlit)
```

---

## 2. Message Broker: Redpanda

**Redpanda** là một Kafka-compatible message broker được xây dựng trên C++, không yêu cầu JVM hay Zookeeper—giảm đáng kể footprint tài nguyên so với Apache Kafka truyền thống (Serafini et al., 2023). Trong đồ án, Redpanda chạy trong một container đơn (`docker.redpanda.com/redpandadata/redpanda:v23.3.18`) với cấu hình `--mode dev-container` và giới hạn 512MB RAM.

Hệ thống khởi tạo 5 Kafka topics khi stack khởi động:

| Topic | Partitions | Nguồn dữ liệu |
|-------|-----------|----------------|
| `olist.orders` | 3 | `olist_orders_dataset.csv` (enriched) |
| `olist.reviews` | 3 | `olist_order_reviews_dataset.csv` |
| `olist.payments` | 3 | Đã được merge vào `olist.orders` |
| `olist.leads` | 2 | `olist_marketing_qualified_leads_dataset.csv` |
| `olist.deals` | 2 | `olist_closed_deals_dataset.csv` |

Việc sử dụng `order_id` làm Kafka message key đảm bảo tất cả events liên quan đến cùng một đơn hàng được định tuyến vào cùng một partition, duy trì thứ tự xử lý (*ordering guarantee*) trong phạm vi một partition (Narkhede et al., 2017, tr. 55–58).

---

## 3. Streaming Producer: Giả Lập Dữ Liệu Thời Gian Thực

### 3.1 Vấn đề với dữ liệu lịch sử

Tập dữ liệu Olist là dữ liệu lịch sử (2016–2018), không phải stream thời gian thực. Để giả lập một hệ thống e-commerce đang hoạt động, producer cần tái tạo hành vi của một nguồn dữ liệu sống (*live data source*). Kreps (2014) mô tả đây là bài toán *event replay*—phát lại lịch sử với tốc độ được kiểm soát để kiểm thử hệ thống streaming.

### 3.2 Hai chế độ hoạt động

**Mode `rate` (mặc định):** Producer gửi đúng `RATE` events/giây (mặc định 2 events/s), shuffle dataset mỗi vòng lặp, và vô hạn—phù hợp để demo dashboard dài hạn.

```
RATE=2  →  120 orders/phút  →  7,200 orders/giờ
```

**Mode `replay`:** Nén thời gian thực theo hệ số `SPEED_FACTOR`. Ví dụ `SPEED_FACTOR=86400` (1 ngày thực = 1 giây) sẽ phát lại toàn bộ ~800 ngày dữ liệu trong ~800 giây.

Phương trình kiểm soát sleep giữa các events:

```
sleep = (data_elapsed / SPEED_FACTOR) − wall_elapsed
```

Trong đó `data_elapsed` là khoảng cách timestamp giữa 2 events liên tiếp trong dataset gốc, `wall_elapsed` là thời gian thực đã trôi qua từ khi producer bắt đầu. Nếu `sleep < 0` (producer bị lag), bỏ qua sleep để bắt kịp.

### 3.3 Enrichment trước khi produce

Để tránh join phức tạp ở phía consumer và dashboard, producer thực hiện **data enrichment** trước khi đưa vào Kafka bằng cách join hai bảng phụ:

1. **`olist_customers_dataset.csv`** → bổ sung `customer_state`, `customer_city` vào mỗi order event.
2. **`olist_order_payments_dataset.csv`** → aggregate `payment_value` (sum) và `payment_type` (sequential=1) vào mỗi order event.

Kết quả: mỗi message trên `olist.orders` topic là một **denormalized event** chứa đủ thông tin để dashboard render ngay mà không cần tra cứu thêm. Đây là pattern *event enrichment at source* được Martin Fowler mô tả trong bối cảnh event-driven architecture (Fowler, 2017).

Payload mẫu một message `olist.orders`:

```json
{
  "order_id": "e481f51cbdc54678b7cc49136f2d6af7",
  "customer_id": "9ef432eb6251297304e76186b10a928d",
  "order_status": "delivered",
  "order_purchase_timestamp": "2017-10-02 10:56:33",
  "customer_state": "SP",
  "customer_city": "sao paulo",
  "payment_value": 141.90,
  "payment_type": "credit_card",
  "_produced_at": "2026-05-26T10:31:42.123456+00:00"
}
```

---

## 4. Streaming Consumer: Micro-Batch với Dual Trigger

### 4.1 Chiến lược micro-batch

Consumer không xử lý từng message riêng lẻ (per-message processing) vì PyIceberg tạo một Parquet file mới cho mỗi lần `append()`—xử lý từng message sẽ tạo ra hàng nghìn file nhỏ (*small file problem*), làm chậm truy vấn đáng kể (Apache Software Foundation, 2024). Thay vào đó, consumer dùng **micro-batch** với hai trigger độc lập:

| Trigger | Điều kiện | Mục đích |
|---------|-----------|---------|
| **Batch size** | Buffer đạt `BATCH_SIZE = 200` messages | Throughput cao khi volume lớn |
| **Time interval** | Buffer không flush trong `FLUSH_INTERVAL = 15s` | Đảm bảo dữ liệu không tồn đọng khi volume thấp |

Pattern này tương đương với *micro-batch streaming* của Apache Spark Structured Streaming (Zaharia et al., 2016), ở đó mỗi micro-batch được xử lý như một batch job nhỏ.

### 4.2 Schema alignment

Do streaming data đến dưới dạng JSON (không có schema tường minh), consumer phải align DataFrame với schema của bảng Iceberg hiện có trước khi append. Quy trình gồm 3 bước:

1. **Bổ sung cột thiếu**: JSON message không chứa một số cột optional (ví dụ `order_delivered_customer_date` với các đơn chưa giao) → thêm vào với giá trị `None`.
2. **Loại bỏ cột thừa**: `_produced_at` (metadata producer) không thuộc schema Bronze → drop.
3. **Ép kiểu theo Iceberg schema**: Dùng `schema_to_pyarrow(iceberg_schema)` của PyIceberg để convert pandas DataFrame sang Arrow Table với đúng kiểu dữ liệu (`int64`, `timestamp[us]`, `string`), tránh lỗi type mismatch khi Parquet writer kiểm tra schema.

```python
arrow_schema = schema_to_pyarrow(iceberg_tbl.schema())
arrow_tbl    = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)
iceberg_tbl.append(arrow_tbl)
```

### 4.3 Delivery semantics

Consumer sử dụng **at-least-once delivery** (`enable.auto.commit = False`, commit thủ công sau mỗi flush thành công). Điều này có nghĩa trong trường hợp crash giữa chừng, một số messages có thể được ghi lại vào Iceberg (duplicates). Đây là trade-off chấp nhận được với Bronze layer vì:

- Bronze là append-only, không có constraint unique.
- Silver layer thực hiện `dropDuplicates()` khi đọc từ Bronze.
- Exactly-once delivery yêu cầu transaction coordinator phức tạp hơn, không phù hợp với phạm vi đồ án (Narkhede et al., 2017, tr. 201–210).

---

## 5. Real-Time Dashboard: Streamlit

### 5.1 Kiến trúc hiển thị

Dashboard được xây dựng trên **Streamlit** với pattern đặc biệt để duy trì trạng thái qua các lần tải lại trang (*page reload*):

```python
@st.cache_resource          # tồn tại suốt vòng đời Streamlit server process
def get_shared_state():
    return {"orders": deque(maxlen=2000), "total_received": 0, ...}

@st.cache_resource          # consumer Kafka khởi tạo một lần duy nhất
def get_consumer():
    ...
```

`st.cache_resource` là cơ chế singleton của Streamlit—hàm chỉ được gọi một lần, kết quả được cache và tái sử dụng cho tất cả sessions và reruns (Streamlit Inc., 2024). Đây là giải pháp cho một hạn chế cơ bản của Streamlit: `st.session_state` bị xóa khi người dùng đóng/mở lại tab, trong khi `st.cache_resource` tồn tại trong memory của server process.

Consumer dùng `auto.offset.reset = earliest` để mỗi khi dashboard khởi động (hoặc Streamlit restart), nó đọc lại toàn bộ messages còn trong Redpanda—đảm bảo người dùng luôn thấy đầy đủ dữ liệu mà không cần chạy lại producer.

### 5.2 Polling và refresh

Mỗi lần Streamlit rerun (tự động mỗi 3 giây):

1. `poll_messages(consumer, max_msgs=300)` poll Kafka trong 0.5 giây, lấy tối đa 300 messages mới.
2. Append vào `shared["orders"]` (deque với maxlen=2000, tự loại bỏ phần tử cũ nhất).
3. Build DataFrame từ deque.
4. Render 4 charts + KPI metrics + bảng orders mới nhất.
5. `time.sleep(3)` → `st.rerun()`.

### 5.3 Nội dung dashboard

| Thành phần | Dữ liệu | Insight |
|------------|---------|---------|
| **KPI metrics** | Total orders, Revenue, Avg value, Delivered, Canceled | Tổng quan nhanh |
| **Revenue by Payment Type** | `payment_type`, `payment_value` | Credit card vs boleto vs voucher |
| **Order Status** (pie) | `order_status` | Tỷ lệ delivered/pending/canceled |
| **Top 10 Customer States** | `customer_state` | Phân bổ địa lý |
| **Cumulative Revenue** (area) | `payment_value` theo thời gian | Xu hướng doanh thu |
| **Latest 20 Orders** (table) | Tất cả fields | Debug / kiểm tra |

---

## 6. Đánh giá và Hạn chế

### Điểm mạnh

- **End-to-end observable**: Toàn bộ pipeline từ producer đến dashboard có thể quan sát qua Redpanda Console (topics, consumer lag) và Prefect UI (flow runs).
- **Schema-safe**: Consumer dùng `schema_to_pyarrow` để đảm bảo type consistency giữa stream và Iceberg—tránh silent data corruption.
- **Idempotent Bronze**: Vì Bronze là append-only và Silver thực hiện dedup, pipeline chịu được at-least-once delivery mà không ảnh hưởng tầng Gold.

### Hạn chế

- **Không stateful processing**: Consumer không thực hiện aggregation hay join theo thời gian (windowed operations). Để làm điều này cần Apache Flink hoặc Spark Structured Streaming với checkpointing (Carbone et al., 2015).
- **Single consumer instance**: Không có consumer group với nhiều instances—không scale horizontally. Trong production, mỗi partition nên có một consumer instance riêng.
- **Dữ liệu giả lập**: Rate mode (`RATE=2`) shuffle ngẫu nhiên qua dataset, phá vỡ temporal ordering tự nhiên. Mode `replay` trung thực hơn nhưng chỉ chạy một lần.

---

## Tài liệu Tham khảo

Apache Software Foundation. (2024). *Apache Iceberg: Small file compaction*. https://iceberg.apache.org/docs/latest/maintenance/

Carbone, P., Katsifodimos, A., Ewen, S., Markl, V., Haridi, S., & Tzoumas, K. (2015). Apache Flink: Stream and batch processing in a single engine. *IEEE Data Engineering Bulletin, 38*(4), 28–38.

Fowler, M. (2017, February 7). *What do you mean by "event-driven"?* martinfowler.com. https://martinfowler.com/articles/201701-event-driven.html

Kleppmann, M. (2017). *Designing data-intensive applications: The big ideas behind reliable, scalable, and maintainable systems*. O'Reilly Media.

Kreps, J. (2014). *I ♥ logs: Event data, stream processing, and data integration*. O'Reilly Media.

Marz, N., & Warren, J. (2015). *Big data: Principles and best practices of scalable real-time data systems*. Manning Publications.

Narkhede, N., Shapira, G., & Palino, T. (2017). *Kafka: The definitive guide*. O'Reilly Media.

Serafini, M., Motiwala, M., Welde, E., & Johnson, A. (2023). Redpanda: A Kafka-compatible streaming data platform. *Proceedings of the VLDB Endowment, 16*(12), 3822–3825. https://doi.org/10.14778/3611540.3611563

Streamlit Inc. (2024). *st.cache_resource: Cache global resources*. https://docs.streamlit.io/library/api-reference/performance/st.cache_resource

Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A., Meng, X., Rosen, J., Venkataraman, S., Franklin, M. J., Ghodsi, A., Gonzalez, J., Shenker, S., & Stoica, I. (2016). Apache Spark: A unified engine for big data processing. *Communications of the ACM, 59*(11), 56–65. https://doi.org/10.1145/2934664

---

*Tài liệu soạn theo chuẩn APA 7th Edition. Các URL truy cập tháng 5 năm 2026.*
