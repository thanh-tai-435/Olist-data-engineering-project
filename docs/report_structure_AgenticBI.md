# Cấu Trúc Báo Cáo — Phiên Bản Agentic BI
**Chủ đề:** Hệ thống Business Intelligence Thông minh với Mô hình Ngôn ngữ Lớn — Từ Truy vấn Ngôn ngữ Tự nhiên đến Phân tích Dữ liệu  
**Người nộp:** [Tên bạn]  
**Trọng tâm:** LLM, Text-to-SQL, Conversational BI, AI Agent

---

## CHƯƠNG 1 — GIỚI THIỆU *(~4 trang)*

### 1.1 Bối cảnh: Khoảng cách giữa Dữ liệu và Quyết định
- Vấn đề: 70% người dùng doanh nghiệp không thể tự truy vấn dữ liệu do rào cản SQL
- Tiến hóa BI: Report → Dashboard → Self-service → Conversational AI
- Agentic BI là gì: hệ thống có thể hiểu câu hỏi, tự viết SQL, tự giải thích kết quả

### 1.2 Mục tiêu Đề tài
- Nghiên cứu và triển khai hệ thống Agentic BI sử dụng LLM (Claude API)
- Đánh giá độ chính xác Text-to-SQL trên dataset e-commerce thực tế
- Tích hợp real-time monitoring và conversational interface

### 1.3 Phạm vi
- Dataset: Olist Brazilian E-Commerce (Gold layer — fct_orders, dim_sellers, dim_customers, fct_funnel)
- LLM: Claude (Anthropic API)
- Interface: Streamlit web application

### 1.4 Câu hỏi Nghiên cứu
1. LLM có thể tạo ra SQL chính xác từ ngôn ngữ tự nhiên với schema phức tạp không?
2. Kỹ thuật nào cải thiện độ chính xác Text-to-SQL nhất?
3. Agentic BI có thể thay thế BI analyst truyền thống trong các tác vụ thường ngày?

### 1.5 Cấu trúc Báo cáo

---

## CHƯƠNG 2 — CƠ SỞ LÝ THUYẾT *(~10 trang)*

### 2.1 Business Intelligence và Tiến hóa sang AI-driven BI
- Traditional BI: OLAP, MDX, pre-built reports
- Self-service BI: Tableau, Power BI
- Agentic BI: LLM-powered, dynamic query generation
- **Bảng:** So sánh 3 thế hệ BI

### 2.2 Mô hình Ngôn ngữ Lớn (Large Language Models)

#### 2.2.1 Kiến trúc Transformer
- Attention mechanism: "Attention is All You Need" (Vaswani et al., 2017)
- Self-attention, multi-head attention
- Pre-training vs Fine-tuning

#### 2.2.2 Từ GPT đến Claude
- GPT family (Brown et al., 2020)
- Constitutional AI và RLHF (Bai et al., 2022)
- Claude: Anthropic's approach to alignment

#### 2.2.3 In-context Learning và Prompt Engineering
- Zero-shot, few-shot learning
- Chain-of-Thought prompting (Wei et al., 2022)
- Role prompting, system instructions

### 2.3 Text-to-SQL: Bài toán và Thách thức

#### 2.3.1 Lịch sử Text-to-SQL
- Semantic parsing → Neural approaches → LLM-based
- Benchmarks: Spider, WikiSQL, BIRD

#### 2.3.2 Thách thức Kỹ thuật
- Schema linking: ánh xạ intent → correct table/column
- Ambiguity resolution: câu hỏi có nhiều cách hiểu
- Multi-hop reasoning: cần join nhiều bảng
- Aggregation inference: tự nhận ra cần GROUP BY / SUM / AVG

#### 2.3.3 Kỹ thuật Cải thiện Độ chính xác
- Schema description trong prompt
- Few-shot examples theo domain
- Self-correction: LLM tự kiểm tra và sửa SQL lỗi
- Retrieval-Augmented Generation (RAG) cho schema

### 2.4 AI Agents và Agentic Workflows
- Định nghĩa Agent: LLM + Tools + Memory + Planning
- ReAct framework: Reasoning + Acting (Yao et al., 2023)
- Tool use: function calling trong Claude API
- Multi-step reasoning cho phân tích dữ liệu phức tạp

### 2.5 Mô hình Dữ liệu Chiều cho BI (Kiến thức nền)
- Star schema: fact tables + dimension tables
- Tại sao dimensional model dễ viết SQL tự nhiên hơn 3NF
- Metadata-rich schema giúp LLM hiểu context
- **Bảng:** Schema Gold layer — columns và business meaning

---

## CHƯƠNG 3 — PHÂN TÍCH YÊU CẦU VÀ THIẾT KẾ HỆ THỐNG *(~6 trang)*

### 3.1 Dataset và Data Foundation
- Olist Gold layer: 4 bảng được chuẩn bị bởi DE pipeline
  - `fct_orders`: ~99K đơn hàng, delivery metrics, revenue
  - `fct_funnel`: ~8K leads, conversion funnel
  - `dim_sellers`: ~3K sellers + performance metrics
  - `dim_customers`: ~96K customers + CLV features
- **Hình:** Star schema — fct_orders ↔ dim_sellers, dim_customers
- Tại sao Gold layer phù hợp cho Agentic BI hơn Bronze/Silver

### 3.2 Yêu cầu Hệ thống Agentic BI
- **Chức năng:**
  - Nhận câu hỏi ngôn ngữ tự nhiên (tiếng Anh/Việt)
  - Tạo SQL phù hợp với schema Gold layer
  - Thực thi query, trả kết quả
  - Giải thích kết quả bằng ngôn ngữ tự nhiên
  - Gợi ý câu hỏi follow-up
- **Phi chức năng:**
  - Response time < 10 giây
  - Xử lý gracefully khi SQL lỗi
  - Hiển thị SQL để người dùng kiểm tra (transparency)

### 3.3 Thiết kế Kiến trúc Agentic BI
- **Hình:** Kiến trúc tổng thể Agentic BI system
  ```
  User Query (NL)
       │
       ▼
  Schema Context Builder  ←── Gold Layer Schema
       │
       ▼
  Claude API (Text-to-SQL)
       │
       ▼
  SQL Validator / Executor (DuckDB/Trino)
       │
       ▼
  Result Interpreter (Claude API)
       │
       ▼
  Streamlit UI (Answer + Chart + SQL)
  ```

### 3.4 Lựa chọn Công nghệ
- **Claude API**: khả năng reasoning mạnh, function calling, long context
- **DuckDB**: embedded SQL engine, đọc trực tiếp Parquet/Iceberg, không cần server
- **Streamlit**: prototype nhanh, built-in charting, session state
- **Bảng:** So sánh Claude vs GPT-4 vs Gemini cho Text-to-SQL

---

## CHƯƠNG 4 — XÂY DỰNG HỆ THỐNG AGENTIC BI *(~14 trang — TRỌNG TÂM)*

### 4.1 Schema Context Engineering *(quan trọng)*

#### 4.1.1 Vấn đề: LLM không biết schema của bạn
- Hallucination khi không có context schema
- Tầm quan trọng của schema description chất lượng cao

#### 4.1.2 Thiết kế Schema Prompt
```
Bảng fct_orders: mỗi dòng = 1 đơn hàng đã đặt
  - order_id (string): mã định danh đơn hàng
  - order_revenue (double): doanh thu đơn hàng (BRL)
  - delivery_status (string): 'delivered'|'early'|'late'|'on_time'
  - customer_state (string): bang của khách hàng (SP, RJ, ...)
  ...
```
- Thêm business context: giải thích ý nghĩa từng cột
- Sample values để LLM hiểu kiểu dữ liệu
- Relationships giữa các bảng

#### 4.1.3 Dynamic Schema Loading
- Đọc schema từ Iceberg metadata (không hardcode)
- Tự động cập nhật khi schema thay đổi

### 4.2 Text-to-SQL với Claude API

#### 4.2.1 System Prompt Design
- Role: "bạn là SQL expert, chuyên về e-commerce analytics"
- Constraints: chỉ đọc (SELECT), không DML
- Output format: JSON `{"sql": "...", "explanation": "..."}`

#### 4.2.2 Few-shot Examples
```
Q: "Tổng doanh thu theo tháng năm 2018?"
A: SELECT order_month, SUM(order_revenue) FROM fct_orders
   WHERE order_month LIKE '2018%' GROUP BY 1 ORDER BY 1

Q: "Top 5 bang có nhiều đơn hàng nhất?"
A: SELECT customer_state, COUNT(*) as orders
   FROM fct_orders GROUP BY 1 ORDER BY 2 DESC LIMIT 5
```

#### 4.2.3 Chain-of-Thought SQL Generation
- Bước 1: Identify entities → bảng nào cần join
- Bước 2: Identify metrics → cần aggregation gì
- Bước 3: Identify filters → WHERE conditions
- Bước 4: Generate SQL
- **Hình:** CoT reasoning flow diagram

### 4.3 SQL Execution và Error Handling

#### 4.3.1 SQL Validator
- Kiểm tra cú pháp trước khi execute
- Giới hạn LIMIT để tránh full scan

#### 4.3.2 Self-Correction Loop
```
SQL lỗi → gửi error message về Claude → Claude tự sửa → thử lại
```
- Tối đa 3 lần retry
- Log lỗi để phân tích
- **Hình:** Self-correction flowchart

#### 4.3.3 DuckDB Execution
- Connect tới Iceberg/Parquet files trực tiếp
- Result → pandas DataFrame → Streamlit chart

### 4.4 Result Interpretation và Natural Language Response

#### 4.4.1 Kết quả → Ngôn ngữ Tự nhiên
- Gửi DataFrame (dạng markdown table) về Claude
- Yêu cầu: tóm tắt insight, highlight anomalies, so sánh

#### 4.4.2 Automatic Chart Suggestion
- LLM suggest loại chart phù hợp (bar, line, pie, scatter)
- Plotly rendering trong Streamlit

#### 4.4.3 Follow-up Question Generation
- Sau mỗi answer, generate 3 câu hỏi tiếp theo liên quan
- Tăng tính khám phá (*exploratory analytics*)

### 4.5 Conversational Memory

#### 4.5.1 Vấn đề: LLM không nhớ lịch sử
- Mỗi API call là stateless
- Cần maintain conversation history

#### 4.5.2 Sliding Window Context
- Giữ N turns gần nhất trong context
- Tóm tắt conversation cũ để tiết kiệm tokens
- **Hình:** Context window management diagram

#### 4.5.3 Agentic BI với Multi-step Reasoning
- Câu hỏi phức tạp cần nhiều queries: "So sánh doanh thu của top 5 sellers năm 2017 và 2018"
- LLM tự phân tách thành sub-questions
- Tổng hợp kết quả từ nhiều queries

### 4.6 Giao diện Người dùng (Streamlit)

#### 4.6.1 Layout và UX Design
- Chat interface: lịch sử conversation
- SQL panel: hiển thị SQL được generate (transparency)
- Chart panel: auto-rendered visualization
- **Hình:** Screenshot giao diện Agentic BI

#### 4.6.2 Ví dụ Hỏi đáp Thực tế
| Câu hỏi | SQL được generate | Kết quả |
|---------|------------------|---------|
| "Doanh thu tháng 11/2017 bao nhiêu?" | `SELECT SUM(order_revenue) ...` | R$ 1,234,567 |
| "Bang nào có tỷ lệ giao trễ cao nhất?" | `SELECT customer_state, AVG(CASE ...) ...` | AM: 32% |
| "Seller nào có review tốt nhất có >100 đơn?" | Multi-join query | seller_id: abc... |

---

## CHƯƠNG 5 — REAL-TIME BI DASHBOARD *(~6 trang)*

### 5.1 Sự Cần Thiết của Real-time Monitoring
- Hạn chế của batch BI: dữ liệu cũ 24-48h
- Streaming BI: insight tức thời cho operations team

### 5.2 Kiến trúc Streaming BI
- Redpanda (Kafka) → Streamlit Consumer
- `st.cache_resource` pattern: shared state across sessions
- **Hình:** Streaming BI data flow

### 5.3 Thiết kế Dashboard Metrics

#### 5.3.1 KPI Metrics (Real-time)
- Orders/phút, Revenue tích lũy, Tỷ lệ delivered, Tỷ lệ cancelled

#### 5.3.2 Charts
- Revenue by Payment Type (bar)
- Order Status Distribution (pie)
- Top 10 Customer States (bar)
- Cumulative Revenue (area chart)

#### 5.3.3 Auto-refresh Pattern
- `time.sleep(3)` + `st.rerun()` loop
- Consumer đọc `earliest` → không mất data khi reload
- **Hình:** Screenshot real-time dashboard

### 5.4 Kết hợp Agentic BI và Real-time Dashboard
- Real-time dashboard: "what is happening NOW?"
- Agentic BI: "why is this happening? what should we do?"
- Tích hợp: user thấy anomaly trên dashboard → hỏi Agentic BI ngay

---

## CHƯƠNG 6 — ĐÁNH GIÁ *(~8 trang — TRỌNG TÂM)*

### 6.1 Phương pháp Đánh giá Text-to-SQL

#### 6.1.1 Bộ câu hỏi kiểm thử
- 30 câu hỏi phân theo độ phức tạp:
  - **Đơn giản** (10): single table, single metric
  - **Trung bình** (10): join 2 bảng, GROUP BY
  - **Phức tạp** (10): multi-join, subquery, window function

#### 6.1.2 Metrics đánh giá
- **Execution Accuracy (EX)**: SQL chạy được và trả kết quả đúng
- **Valid SQL Rate**: tỷ lệ SQL hợp lệ (không syntax error)
- **Self-correction Rate**: tỷ lệ sửa được sau lỗi lần 1

### 6.2 Kết quả Thực nghiệm
- **Bảng:** Accuracy theo độ phức tạp
  | Độ phức tạp | Valid SQL | Correct Result | Self-corrected |
  |-------------|-----------|----------------|----------------|
  | Đơn giản    | 10/10     | 9/10           | 1/1            |
  | Trung bình  | 10/10     | 8/10           | 1/2            |
  | Phức tạp    | 9/10      | 6/10           | 2/3            |
  | **Tổng**    | **96.7%** | **76.7%**      | **~75%**       |

*(Điền số liệu thực tế khi chạy)*

### 6.3 Phân tích Lỗi
- Lỗi thường gặp: sai tên column, thiếu JOIN condition, sai aggregation
- **Bảng:** Phân loại lỗi × tần suất × nguyên nhân
- Case study: câu hỏi nào khó nhất và tại sao

### 6.4 Ảnh hưởng của Prompt Engineering
- Thí nghiệm A/B: zero-shot vs few-shot vs CoT
- **Bảng:** So sánh accuracy 3 kỹ thuật prompt
- Kết luận: few-shot + schema description mang lại cải thiện lớn nhất

### 6.5 Đánh giá Người dùng (User Study nhỏ)
- 5 người không biết SQL thử hệ thống
- Đánh giá: usefulness, ease of use, trust in results
- **Bảng:** SUS score hoặc Likert scale

### 6.6 Hạn chế
- Chưa xử lý câu hỏi ngoài domain (hallucination)
- Câu hỏi rất phức tạp (>3 bảng) còn lỗi cao
- Latency: ~3-5s per query (API roundtrip)

---

## CHƯƠNG 7 — KẾT LUẬN *(~2 trang)*

### 7.1 Tóm tắt Đóng góp
- Hệ thống Agentic BI hoàn chỉnh với LLM trên dữ liệu e-commerce thực tế
- Đánh giá định lượng accuracy Text-to-SQL
- Real-time dashboard tích hợp streaming

### 7.2 Bài học về LLM cho Analytics
- Schema quality ảnh hưởng trực tiếp đến LLM performance
- Few-shot examples từ cùng domain quan trọng hơn model size
- Self-correction loop là "must-have" cho production system

### 7.3 Hướng Nghiên cứu Tiếp theo
- Fine-tuning LLM trên dataset SQL e-commerce
- Multi-agent: một agent viết SQL, một agent kiểm tra
- Voice interface: speech → Agentic BI

---

## TÀI LIỆU THAM KHẢO

*(Tối thiểu 20 nguồn — ưu tiên: NeurIPS, ACL, EMNLP, VLDB, ACM + sách O'Reilly)*

Bao gồm: Vaswani et al. (2017) Transformer, Brown et al. (2020) GPT-3,
Bai et al. (2022) Constitutional AI, Wei et al. (2022) Chain-of-Thought,
Yao et al. (2023) ReAct, Yu et al. (2018) Spider benchmark,
Li et al. (2023) BIRD benchmark, Rajkumar et al. (2022) Text-to-SQL with LLM,
Armbrust et al. (2021) Lakehouse, Kimball & Ross (2013), Reis & Housley (2022), ...

---

## PHỤ LỤC

- **Phụ lục A:** System prompt đầy đủ cho Text-to-SQL
- **Phụ lục B:** 30 câu hỏi kiểm thử và kết quả chi tiết
- **Phụ lục C:** Schema description đầy đủ của Gold layer
- **Phụ lục D:** Hướng dẫn cài đặt Agentic BI

---

> **Ước tính tổng:** ~58 trang nội dung + phụ lục  
> **Hình/Bảng dự kiến:** ~20 hình, ~15 bảng
