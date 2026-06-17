"""
Core agent logic:
  - LLM call abstraction (Anthropic / OpenRouter / Groq)
  - Intent handlers: SMALLTALK, FOLLOWUP, DATA_QUERY
  - SQL generation with self-correction (up to MAX_RETRIES)
  - Post-execution result summarization
"""
import json
import re
import duckdb
import pandas as pd

from config import (
    AI_PROVIDER, ANTHROPIC_KEY, CLAUDE_MODEL,
    OPENROUTER_KEY, OPENROUTER_MODEL,
    GROQ_KEY, GROQ_MODEL,
    GOLD_TABLES, MAX_ROWS, MAX_HISTORY, MAX_RETRIES,
)

# ── Prompts ───────────────────────────────────────────────────────────────────

_SYSTEM_SQL = """\
Bạn là SQL analyst chuyên về dữ liệu e-commerce Olist Brazil.

NHIỆM VỤ: Nhận câu hỏi, sinh SQL SELECT để trả lời, trả về JSON.

QUY TẮC SQL:
- Chỉ dùng SELECT. Không INSERT/UPDATE/DELETE/DROP/CREATE.
- Tự động thêm LIMIT {max_rows} nếu query không có LIMIT.
- Tên bảng: {table_names}
- Tiền tệ là BRL (R$) — luôn ghi rõ đơn vị trong explanation.
- Group theo tháng: strftime(purchased_at, '%Y-%m') hoặc date_trunc('month', col).
- Explanation bằng tiếng Việt, ngắn gọn.

QUAN TRỌNG — SENTIMENT:
- Khi câu hỏi liên quan đến sentiment/cảm xúc/đánh giá (positive/negative/neutral), LUÔN dùng bảng `review_sentiment` (cột overall_sentiment, product_quality_sentiment, delivery_speed_sentiment, seller_service_sentiment, price_value_sentiment).
- KHÔNG dùng review_score từ fct_orders để suy ra sentiment — đó là số sao, không phải kết quả model ABSA.
- Join: review_sentiment.order_id = fct_orders.order_id

OUTPUT — chỉ JSON thuần, không markdown:
{{"sql": "SELECT ...", "explanation": "...", "chart_type": "bar|line|pie|scatter|none"}}

CHART TYPE:
- bar   : so sánh danh mục (top N, phân bố theo state/seller)
- line  : xu hướng thời gian (theo tháng/năm)
- pie   : tỷ lệ phần trăm (% payment type, % status, % churn)
- scatter: tương quan 2 biến số
- none  : bảng nhiều cột hoặc câu trả lời không cần chart

SCHEMA:
{schema}

FEW-SHOT EXAMPLES:
Q: Doanh thu theo tháng năm 2018?
A: {{"sql": "SELECT strftime(purchased_at, '%Y-%m') AS month, ROUND(SUM(payment_value),2) AS revenue_brl FROM fct_orders WHERE purchased_at >= '2018-01-01' AND purchased_at < '2019-01-01' GROUP BY 1 ORDER BY 1", "explanation": "Tổng doanh thu (BRL) theo từng tháng năm 2018.", "chart_type": "line"}}

Q: Top 10 bang có nhiều đơn hàng nhất?
A: {{"sql": "SELECT customer_state, COUNT(*) AS orders FROM fct_orders GROUP BY customer_state ORDER BY orders DESC LIMIT 10", "explanation": "10 bang Brazil có số lượng đơn hàng cao nhất.", "chart_type": "bar"}}

Q: Tỷ lệ các loại thanh toán?
A: {{"sql": "SELECT payment_type, COUNT(*) AS cnt FROM fct_orders WHERE payment_type IS NOT NULL GROUP BY payment_type ORDER BY cnt DESC", "explanation": "Số đơn hàng theo từng phương thức thanh toán.", "chart_type": "pie"}}

Q: Seller nào có review tốt nhất với hơn 100 đơn?
A: {{"sql": "SELECT seller_id, seller_state, avg_review_score, delivered_orders, ROUND(total_revenue,2) AS revenue_brl FROM dim_sellers WHERE delivered_orders > 100 ORDER BY avg_review_score DESC LIMIT 10", "explanation": "Top 10 seller có điểm đánh giá trung bình cao nhất (>100 đơn).", "chart_type": "bar"}}

Q: Phân tích churn?
A: {{"sql": "SELECT is_churned, COUNT(*) AS customers, ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(),2) AS pct FROM dim_customers GROUP BY is_churned", "explanation": "Tỷ lệ khách hàng churned (không mua trong 90 ngày) so với tổng.", "chart_type": "pie"}}

Q: Tỷ lệ giao hàng trễ theo bang?
A: {{"sql": "SELECT customer_state, COUNT(*) AS total, SUM(CASE WHEN delivery_status='late' THEN 1 ELSE 0 END) AS late_orders, ROUND(SUM(CASE WHEN delivery_status='late' THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS late_pct FROM fct_orders GROUP BY customer_state ORDER BY late_pct DESC LIMIT 15", "explanation": "Tỷ lệ % đơn hàng giao trễ theo từng bang.", "chart_type": "bar"}}

Q: So sánh doanh thu positive và negative theo tháng 2017?
A: {{"sql": "SELECT strftime(o.purchased_at, '%Y-%m') AS month, ROUND(SUM(CASE WHEN r.overall_sentiment='positive' THEN o.payment_value ELSE 0 END),2) AS pos_revenue_brl, ROUND(SUM(CASE WHEN r.overall_sentiment='negative' THEN o.payment_value ELSE 0 END),2) AS neg_revenue_brl FROM review_sentiment r JOIN fct_orders o ON r.order_id = o.order_id WHERE o.purchased_at >= '2017-01-01' AND o.purchased_at < '2018-01-01' GROUP BY 1 ORDER BY 1", "explanation": "Doanh thu (BRL) từ đơn hàng có review positive vs negative theo tháng năm 2017, dựa trên kết quả mô hình ABSA.", "chart_type": "line"}}

Q: Tỷ lệ sentiment của review?
A: {{"sql": "SELECT overall_sentiment, COUNT(*) AS reviews, ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(),2) AS pct FROM review_sentiment GROUP BY overall_sentiment ORDER BY reviews DESC", "explanation": "Phân bố sentiment (positive/neutral/negative) từ mô hình ABSA trên toàn bộ reviews.", "chart_type": "pie"}}

Q: Bang nào có delivery_speed negative nhiều nhất?
A: {{"sql": "SELECT o.customer_state, COUNT(*) AS total, SUM(CASE WHEN r.delivery_speed_sentiment='negative' THEN 1 ELSE 0 END) AS neg_delivery, ROUND(SUM(CASE WHEN r.delivery_speed_sentiment='negative' THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS neg_pct FROM review_sentiment r JOIN fct_orders o ON r.order_id = o.order_id GROUP BY o.customer_state HAVING COUNT(*) >= 50 ORDER BY neg_pct DESC LIMIT 10", "explanation": "Top bang có tỷ lệ review delivery_speed negative cao nhất (theo mô hình ABSA).", "chart_type": "bar"}}
"""

_SYSTEM_SMALLTALK = """\
Bạn là trợ lý phân tích dữ liệu Olist — nền tảng e-commerce Brazil.
Hãy trả lời câu hỏi ngắn gọn, thân thiện, bằng tiếng Việt.
Nhắc người dùng có thể hỏi về: đơn hàng, doanh thu, khách hàng, sellers, giao hàng, marketing funnel.
"""

_SYSTEM_FOLLOWUP = """\
Bạn là SQL analyst chuyên về dữ liệu Olist Brazil.
Dựa trên lịch sử hội thoại, hãy diễn giải và phân tích kết quả hoặc trả lời câu hỏi ngắn.
Trả lời bằng tiếng Việt, ngắn gọn, nêu rõ số liệu cụ thể.
Không được bịa số — chỉ dùng dữ liệu đã có trong lịch sử hội thoại.
"""

_SYSTEM_SUMMARIZE = """\
Bạn là business analyst. Dựa trên câu hỏi và kết quả truy vấn, viết 2-3 câu insight bằng tiếng Việt.

YÊU CẦU:
- Nêu rõ con số quan trọng nhất với đơn vị đầy đủ (tiền: R$, tỷ lệ: %, thời gian: ngày).
- Định dạng tiền BRL: >1 tỷ → "X,XX tỷ R$", >1 triệu → "X,XX triệu R$", nhỏ hơn → "R$X.XX".
- Highlight giá trị cao nhất / thấp nhất / bất thường nếu có.
- KHÔNG dùng: "có thể", "dường như", "có lẽ", "khoảng", "tương đối".
- KHÔNG mô tả lại SQL hay cột dữ liệu — chỉ nói về insight.
- Tối đa 3 câu.
"""

_FIX_MSG = """\
SQL sau gặp lỗi khi thực thi trên DuckDB:

SQL:
{sql}

Lỗi:
{error}

Hãy sửa lại SQL. Chỉ trả về JSON (không markdown):
{{"sql": "...", "explanation": "...", "chart_type": "bar|line|pie|scatter|none"}}"""


# ── LLM abstraction ───────────────────────────────────────────────────────────

def _call_llm(messages: list, system: str, max_tokens: int = 1024) -> str:
    """Call LLM. Primary: Anthropic Claude SDK. Fallback: openrouter / groq via HTTP."""
    if AI_PROVIDER == "anthropic":
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        resp = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=max_tokens, system=system,
            messages=messages, temperature=0,
        )
        return resp.content[0].text

    # Fallback: OpenAI-compatible endpoints (openrouter / groq)
    import requests
    if AI_PROVIDER == "openrouter":
        url, key, model = "https://openrouter.ai/api/v1/chat/completions", OPENROUTER_KEY, OPENROUTER_MODEL
    else:
        url, key, model = "https://api.groq.com/openai/v1/chat/completions", GROQ_KEY, GROQ_MODEL

    all_msgs = ([{"role": "system", "content": system}] if system else []) + messages
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={"model": model, "messages": all_msgs, "max_tokens": max_tokens, "temperature": 0},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _parse_json(text: str) -> dict:
    text = re.sub(r"```(?:json)?\s*", "", text).strip().rstrip("`")
    m = re.search(r"\{.*\}", text, re.DOTALL)
    return json.loads(m.group() if m else text)


# ── Intent handlers ───────────────────────────────────────────────────────────

def handle_smalltalk(question: str) -> str:
    """Short LLM call for greetings and system questions."""
    return _call_llm(
        [{"role": "user", "content": question}],
        _SYSTEM_SMALLTALK,
        max_tokens=256,
    )


def handle_followup(question: str, history: list) -> str:
    """Context-aware response using conversation history."""
    msgs = list(history[-MAX_HISTORY:]) + [{"role": "user", "content": question}]
    return _call_llm(msgs, _SYSTEM_FOLLOWUP, max_tokens=400)


# ── SQL generation + self-correction ─────────────────────────────────────────

def generate_and_execute(
    question: str,
    history: list,
    schema: str,
    con: duckdb.DuckDBPyConnection,
) -> dict:
    """
    Generate SQL via LLM → execute on DuckDB → self-correct up to MAX_RETRIES times.
    Returns: {sql, explanation, chart_type, result_df, error}
    """
    system = _SYSTEM_SQL.format(
        max_rows=MAX_ROWS,
        table_names=", ".join(GOLD_TABLES),
        schema=schema,
    )
    base_msgs = list(history[-MAX_HISTORY:]) + [{"role": "user", "content": question}]

    raw_sql    = ""
    prev_raw   = ""
    last_error = ""

    for attempt in range(MAX_RETRIES):
        try:
            if attempt == 0:
                msgs = base_msgs
            else:
                msgs = base_msgs + [
                    {"role": "assistant", "content": prev_raw},
                    {"role": "user",      "content": _FIX_MSG.format(sql=raw_sql, error=last_error)},
                ]

            raw      = _call_llm(msgs, system)
            prev_raw = raw
            parsed   = _parse_json(raw)
            raw_sql  = parsed.get("sql", "").strip()

            if not raw_sql:
                return {"error": "Không sinh được SQL.", "sql": "", "explanation": raw,
                        "chart_type": "none", "result_df": None}

            if "limit" not in raw_sql.lower():
                raw_sql += f" LIMIT {MAX_ROWS}"
            parsed["sql"] = raw_sql

            df = con.execute(raw_sql).df()
            return {**parsed, "result_df": df, "error": None}

        except json.JSONDecodeError as e:
            return {"error": f"Không parse được JSON từ LLM: {e}", "sql": raw_sql,
                    "explanation": "", "chart_type": "none", "result_df": None}
        except Exception as e:
            last_error = str(e)
            if attempt == MAX_RETRIES - 1:
                return {"error": f"SQL thất bại sau {MAX_RETRIES} lần thử: {last_error}",
                        "sql": raw_sql, "explanation": "", "chart_type": "none", "result_df": None}

    return {"error": "Không thực thi được.", "sql": "", "explanation": "",
            "chart_type": "none", "result_df": None}


# ── Post-execution summarization ──────────────────────────────────────────────

def summarize_result(question: str, df: pd.DataFrame) -> str:
    """
    Call LLM to convert raw query results into business insight.
    Only called when df has data; returns "" on failure.
    """
    if df is None or df.empty:
        return ""

    # Build compact data preview (max 10 rows to save tokens)
    preview = df.head(10).to_string(index=False)
    n_total = len(df)

    prompt = (
        f"Câu hỏi: {question}\n\n"
        f"Kết quả ({n_total} dòng, hiển thị tối đa 10):\n{preview}"
    )
    try:
        return _call_llm(
            [{"role": "user", "content": prompt}],
            _SYSTEM_SUMMARIZE,
            max_tokens=300,
        )
    except Exception:
        return ""


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(
    intent: str,
    question: str,
    history: list,
    schema: str,
    con: duckdb.DuckDBPyConnection,
) -> dict:
    """
    Dispatch based on intent, return a unified result dict:
      {type, text, sql, explanation, chart_type, result_df, summary, error}
    """
    if intent == "SMALLTALK":
        text = handle_smalltalk(question)
        return {"type": "SMALLTALK", "text": text, "sql": None, "explanation": None,
                "chart_type": "none", "result_df": None, "summary": None, "error": None}

    if intent == "FOLLOWUP":
        text = handle_followup(question, history)
        return {"type": "FOLLOWUP", "text": text, "sql": None, "explanation": None,
                "chart_type": "none", "result_df": None, "summary": None, "error": None}

    # DATA_QUERY
    res = generate_and_execute(question, history, schema, con)
    summary = ""
    if not res.get("error") and res.get("result_df") is not None:
        summary = summarize_result(question, res["result_df"])

    return {
        "type":       "DATA_QUERY",
        "text":       None,
        "sql":        res.get("sql"),
        "explanation": res.get("explanation"),
        "chart_type": res.get("chart_type", "none"),
        "result_df":  res.get("result_df"),
        "summary":    summary,
        "error":      res.get("error"),
    }
