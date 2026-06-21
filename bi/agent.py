"""
Core agent logic:
  - Anthropic: native tool_use agentic loop (multi-step, self-correcting)
  - OpenRouter / Groq: text-based SQL fallback with self-correction
  - Intent handlers: SMALLTALK, FOLLOWUP, DATA_QUERY
"""
import json
import re
from typing import Callable, Optional

import duckdb
import pandas as pd
from config import (
    AI_PROVIDER,
    ANTHROPIC_KEY,
    CLAUDE_MODEL,
    GOLD_TABLES,
    GROQ_KEY,
    GROQ_MODEL,
    MAX_HISTORY,
    MAX_RETRIES,
    MAX_ROWS,
    OPENROUTER_KEY,
    OPENROUTER_MODEL,
)

# ── Tool definition (Anthropic tool_use API) ──────────────────────────────────

_TOOLS = [
    {
        "name": "query_database",
        "description": (
            "Execute a SQL SELECT query on the Olist Gold tables via DuckDB. "
            "Call one or more times to answer the user's question. "
            "For complex analysis, break into multiple targeted sub-queries — "
            "each focusing on one angle (revenue, reviews, delivery, funnel, etc.)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "Valid DuckDB SELECT statement. Include LIMIT to cap rows.",
                },
                "explanation": {
                    "type": "string",
                    "description": "One-sentence Vietnamese description of what this query answers.",
                },
                "chart_type": {
                    "type": "string",
                    "enum": ["bar", "line", "pie", "scatter", "none"],
                    "description": (
                        "Best chart to visualize the result: "
                        "bar=category comparison, line=time trend, "
                        "pie=percentage breakdown, scatter=correlation, none=table."
                    ),
                },
            },
            "required": ["sql", "explanation", "chart_type"],
        },
    }
]

# ── Prompts ───────────────────────────────────────────────────────────────────

_SYSTEM_TOOL = """\
Bạn là SQL analyst chuyên phân tích dữ liệu e-commerce Olist Brazil.

NHIỆM VỤ: Dùng tool `query_database` để trả lời câu hỏi. Sau khi có đủ data, viết 2-3 câu insight bằng tiếng Việt.

QUY TẮC SQL:
- Chỉ SELECT. Tuyệt đối không INSERT/UPDATE/DELETE/DROP/CREATE.
- Tự động thêm LIMIT {max_rows} nếu query không có LIMIT.
- Bảng khả dụng: {table_names}
- Tiền tệ BRL (R$) — ghi rõ đơn vị trong explanation.
- Group theo tháng: strftime(purchased_at, '%Y-%m') hoặc date_trunc('month', col).

KHI NÀO GỌI NHIỀU QUERY:
- Câu hỏi "phân tích toàn diện", "so sánh X và Y", "top N với chi tiết" → gọi từng query riêng.
- Ví dụ: "Top 5 seller" → query 1: doanh thu, query 2: review score, query 3: tỷ lệ giao trễ.
- SQL lỗi → tự sửa và gọi lại (Claude thấy ERROR trong kết quả tool).

SENTIMENT:
- Dùng bảng `review_sentiment` (cột: overall_sentiment, product_quality_sentiment, delivery_speed_sentiment, seller_service_sentiment, price_value_sentiment).
- KHÔNG dùng review_score để suy ra sentiment.
- Join: review_sentiment.order_id = fct_orders.order_id

INSIGHT CUỐI (sau khi gọi tool xong):
- Nêu số liệu cụ thể với đơn vị đầy đủ (R$, %, ngày).
- Tiền BRL: >1 tỷ → "X,XX tỷ R$", >1 triệu → "X,XX triệu R$".
- Không dùng: "có thể", "dường như", "khoảng".
- Tối đa 3 câu.

SCHEMA:
{schema}
"""

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
- Khi câu hỏi liên quan đến sentiment/cảm xúc/đánh giá (positive/negative/neutral), LUÔN dùng bảng `review_sentiment`.
- Cột: overall_sentiment, product_quality_sentiment, delivery_speed_sentiment, seller_service_sentiment, price_value_sentiment.
- KHÔNG dùng review_score từ fct_orders để suy ra sentiment.
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

Q: Tỷ lệ sentiment của review?
A: {{"sql": "SELECT overall_sentiment, COUNT(*) AS reviews, ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(),2) AS pct FROM review_sentiment GROUP BY overall_sentiment ORDER BY reviews DESC", "explanation": "Phân bố sentiment từ mô hình ABSA.", "chart_type": "pie"}}
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


# ── LLM abstraction (text mode) ───────────────────────────────────────────────

def _call_llm(messages: list, system: str, max_tokens: int = 1024) -> str:
    if AI_PROVIDER == "anthropic":
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        resp = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=max_tokens, system=system,
            messages=messages, temperature=0,
        )
        return resp.content[0].text

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
    start = text.find("{")
    if start == -1:
        raise json.JSONDecodeError("No JSON object found", text, 0)
    obj, _ = json.JSONDecoder().raw_decode(text, start)
    return obj


# ── Intent handlers ───────────────────────────────────────────────────────────

def handle_smalltalk(question: str) -> str:
    return _call_llm([{"role": "user", "content": question}], _SYSTEM_SMALLTALK, max_tokens=256)


def handle_followup(question: str, history: list) -> str:
    msgs = list(history[-MAX_HISTORY:]) + [{"role": "user", "content": question}]
    return _call_llm(msgs, _SYSTEM_FOLLOWUP, max_tokens=400)


# ── Tool use — Anthropic native agentic loop ──────────────────────────────────

def _run_tool_use(
    question: str,
    history: list,
    schema: str,
    con: duckdb.DuckDBPyConnection,
    on_query: Optional[Callable] = None,
) -> dict:
    """
    Agentic loop using Claude's native tool_use API.

    Claude decides when and how many times to call query_database.
    Each SQL error is visible to Claude in the tool result, so it
    self-corrects naturally without explicit retry logic.

    Returns: {sql, explanation, chart_type, result_df, all_queries, summary, error}
    """
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

    system = _SYSTEM_TOOL.format(
        max_rows=MAX_ROWS,
        table_names=", ".join(GOLD_TABLES),
        schema=schema,
    )
    messages = list(history[-MAX_HISTORY:]) + [{"role": "user", "content": question}]

    all_queries: list[dict] = []
    final_insight = ""
    _MAX_ITERS = 8  # safety cap against infinite loops

    for _ in range(_MAX_ITERS):
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=4096,
            system=system,
            tools=_TOOLS,
            messages=messages,
            temperature=0,
        )

        # Collect any text content in this turn (appears in end_turn or alongside tool_use)
        turn_text = " ".join(
            b.text for b in response.content if hasattr(b, "text") and b.text
        ).strip()

        if response.stop_reason == "end_turn":
            final_insight = turn_text
            break

        if response.stop_reason != "tool_use":
            # Unexpected stop (max_tokens, etc.) — use whatever text we have
            final_insight = turn_text
            break

        # ── Process all tool calls in this turn ──────────────────────────────
        messages.append({"role": "assistant", "content": response.content})
        tool_results = []

        for block in response.content:
            if block.type != "tool_use":
                continue

            sql         = block.input.get("sql", "").strip()
            explanation = block.input.get("explanation", "")
            chart_type  = block.input.get("chart_type", "none")

            if sql and "limit" not in sql.lower():
                sql += f" LIMIT {MAX_ROWS}"

            if on_query:
                on_query(len(all_queries) + 1, sql, explanation)

            try:
                df = con.execute(sql).df()
                # Send a compact preview back to Claude (not the full DataFrame)
                preview = df.head(50).to_string(index=False)
                result_content = f"OK — {len(df)} rows returned.\n\n{preview}"
                all_queries.append({
                    "sql": sql, "explanation": explanation,
                    "chart_type": chart_type, "result_df": df, "error": None,
                })
            except Exception as exc:
                # Claude sees the error and can self-correct on next iteration
                result_content = f"ERROR: {exc}"
                all_queries.append({
                    "sql": sql, "explanation": explanation,
                    "chart_type": chart_type, "result_df": None, "error": str(exc),
                })

            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": result_content,
            })

        messages.append({"role": "user", "content": tool_results})

    if not all_queries:
        return {
            "sql": "", "explanation": final_insight, "chart_type": "none",
            "result_df": None, "all_queries": [], "summary": final_insight,
            "error": "Claude không thực thi query nào.",
        }

    # Primary result = last query that succeeded (for chart + data display)
    last_ok = next(
        (q for q in reversed(all_queries) if q["result_df"] is not None),
        all_queries[-1],
    )
    return {
        "sql":         last_ok["sql"],
        "explanation": last_ok["explanation"],
        "chart_type":  last_ok["chart_type"],
        "result_df":   last_ok["result_df"],
        "all_queries": all_queries,
        "summary":     final_insight,
        "error":       None,
    }


# ── Text-based fallback (OpenRouter / Groq) ───────────────────────────────────

def _generate_text_based(
    question: str,
    history: list,
    schema: str,
    con: duckdb.DuckDBPyConnection,
) -> dict:
    """Single-shot text prompt → JSON → SQL, with self-correction retries."""
    system = _SYSTEM_SQL.format(
        max_rows=MAX_ROWS,
        table_names=", ".join(GOLD_TABLES),
        schema=schema,
    )
    base_msgs = list(history[-MAX_HISTORY:]) + [{"role": "user", "content": question}]

    raw_sql = ""
    prev_raw = ""
    last_error = ""

    for attempt in range(MAX_RETRIES):
        try:
            if attempt == 0:
                msgs = base_msgs
            else:
                msgs = base_msgs + [
                    {"role": "assistant", "content": prev_raw},
                    {"role": "user", "content": _FIX_MSG.format(sql=raw_sql, error=last_error)},
                ]

            raw      = _call_llm(msgs, system)
            prev_raw = raw
            parsed   = _parse_json(raw)
            raw_sql  = parsed.get("sql", "").strip()

            if not raw_sql:
                return {"error": "Không sinh được SQL.", "sql": "", "explanation": raw,
                        "chart_type": "none", "result_df": None, "all_queries": []}

            if "limit" not in raw_sql.lower():
                raw_sql += f" LIMIT {MAX_ROWS}"
            parsed["sql"] = raw_sql

            df = con.execute(raw_sql).df()
            return {**parsed, "result_df": df, "all_queries": [], "error": None}

        except json.JSONDecodeError as e:
            return {"error": f"Không parse được JSON từ LLM: {e}", "sql": raw_sql,
                    "explanation": "", "chart_type": "none", "result_df": None, "all_queries": []}
        except Exception as e:
            last_error = str(e)
            if attempt == MAX_RETRIES - 1:
                return {"error": f"SQL thất bại sau {MAX_RETRIES} lần thử: {last_error}",
                        "sql": raw_sql, "explanation": "", "chart_type": "none",
                        "result_df": None, "all_queries": []}

    return {"error": "Không thực thi được.", "sql": "", "explanation": "",
            "chart_type": "none", "result_df": None, "all_queries": []}


def _summarize_result(question: str, df: pd.DataFrame) -> str:
    """Fallback-path only: call LLM to generate business insight from raw results."""
    if df is None or df.empty:
        return ""
    preview = df.head(10).to_string(index=False)
    prompt = f"Câu hỏi: {question}\n\nKết quả ({len(df)} dòng, tối đa 10):\n{preview}"
    try:
        return _call_llm([{"role": "user", "content": prompt}], _SYSTEM_SUMMARIZE, max_tokens=300)
    except Exception:
        return ""


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(
    intent: str,
    question: str,
    history: list,
    schema: str,
    con: duckdb.DuckDBPyConnection,
    on_query: Optional[Callable] = None,
) -> dict:
    """
    Dispatch by intent. Returns:
      {type, text, sql, explanation, chart_type, result_df, all_queries, summary, error}

    DATA_QUERY + Anthropic  → tool_use agentic loop (multi-step, self-correcting)
    DATA_QUERY + other      → text-based single-shot with retries
    """
    _empty = {"sql": None, "explanation": None, "chart_type": "none",
              "result_df": None, "all_queries": [], "summary": None, "error": None}

    if intent == "SMALLTALK":
        return {**_empty, "type": "SMALLTALK", "text": handle_smalltalk(question)}

    if intent == "FOLLOWUP":
        return {**_empty, "type": "FOLLOWUP", "text": handle_followup(question, history)}

    # DATA_QUERY ──────────────────────────────────────────────────────────────
    if AI_PROVIDER == "anthropic":
        res = _run_tool_use(question, history, schema, con, on_query=on_query)
    else:
        res = _generate_text_based(question, history, schema, con)
        if not res.get("error") and res.get("result_df") is not None:
            res["summary"] = _summarize_result(question, res["result_df"])

    return {
        "type":        "DATA_QUERY",
        "text":        None,
        "sql":         res.get("sql"),
        "explanation": res.get("explanation"),
        "chart_type":  res.get("chart_type", "none"),
        "result_df":   res.get("result_df"),
        "all_queries": res.get("all_queries", []),
        "summary":     res.get("summary"),
        "error":       res.get("error"),
    }
