# ============================================================
# agent.py v5 — Intent Classification + Memory + Anti-hallucination
#
# Luồng xử lý:
#   1. Validate input (validator.py)
#   2. CLASSIFY INTENT → 4 loại:
#      a) SMALLTALK   → trả lời text, không SQL
#      b) FOLLOWUP    → trả lời từ history, không SQL mới
#      c) OUT_OF_SCOPE→ từ chối lịch sự
#      d) DATA_QUERY  → sinh SQL → thực thi → summarize
#   3. Memory: inject 6 turns gần nhất vào mọi LLM call
# ============================================================

from __future__ import annotations

import re
import unicodedata
from typing import Generator

import pandas as pd
from config import GROQ_API_KEY, GROQ_MODEL, MAX_SQL_RETRIES
from database import execute_query
from groq import Groq
from validator import build_schema_context, route_question, validate_input

# ---- Conversation memory ----------------------------------------
MAX_HISTORY = 6
_history: list[dict] = []

def get_history() -> list[dict]:
    return list(_history[-MAX_HISTORY:])

def push(role: str, content: str) -> None:
    _history.append({"role": role, "content": content})
    if len(_history) > MAX_HISTORY * 2:
        del _history[:-MAX_HISTORY]

def clear_history() -> None:
    _history.clear()


class AgentEvent:
    def __init__(self, stage: str, message: str, payload: dict | None = None):
        self.stage, self.message, self.payload = stage, message, payload or {}


_client = Groq(api_key=GROQ_API_KEY)

def _llm(system: str, messages: list[dict], max_tokens=1024) -> str:
    r = _client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role":"system","content":system}] + messages,
        max_tokens=max_tokens, temperature=0.0,
    )
    return r.choices[0].message.content.strip()

def _no_acc(t: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD",t) if not unicodedata.combining(c)).lower()

# ================================================================
# INTENT CLASSIFIER — bước quan trọng nhất, quyết định luồng xử lý
# ================================================================

_SMALLTALK_SIGNALS = [
    "ban co hieu", "bạn có hiểu", "ban hieu khong", "bạn hiểu không",
    "ban la gi", "bạn là gì", "ban ten gi", "bạn tên gì",
    "ban co the", "xin chao", "hello", "hi ", "chao ban",
    "cam on", "cảm ơn", "thank", "ok ban", "tuyet voi",
    "ban gioi", "bạn giỏi", "hay qua", "tot qua",
]

_FOLLOWUP_SIGNALS = [
    "y nghia", "ý nghĩa", "bai tren", "bài trên", "ket qua tren", "kết quả trên",
    "hinh tren", "hình trên", "dieu nay", "điều này", "so lieu tren",
    "nen lam gi", "nên làm gì", "hanh dong gi", "hành động gì",
    "co the lam gi", "có thể làm gì", "khuyen nghi", "khuyến nghị",
    "goi y gi", "gợi ý gì", "cai thien", "cải thiện",
    "chien luoc", "chiến lược", "ke hoach", "kế hoạch",
    "de xuat", "đề xuất", "tu do", "từ đó", "bai nay cho thay",
    "tai sao", "tại sao", "ly do gi", "nguyen nhan",
    "giai thich", "giải thích", "co nghia la", "tuc la",
    "nhu vay thi", "nhu the thi", "tu thong tin tren",
    "danh gia them", "phan tich them", "con so nay",
    "thong tin vua roi", "du lieu tren", "ket qua vua",
    "tu hinh", "từ hình", "tu bieu do", "từ biểu đồ",
]

def _classify_intent(question: str, has_history: bool) -> str:
    """
    Trả về: 'smalltalk' | 'followup' | 'data_query'
    Phân loại trước khi làm bất cứ điều gì khác.
    """
    q = _no_acc(question)

    # Smalltalk: hỏi về bản thân AI, lời chào thuần túy
    if any(s in q for s in _SMALLTALK_SIGNALS):
        return "smalltalk"

    # Follow-up: câu hỏi kế thừa context, chỉ valid nếu đã có history
    if has_history and any(s in q for s in _FOLLOWUP_SIGNALS):
        return "followup"

    return "data_query"


# ================================================================
# PROMPTS
# ================================================================

_SQL_SYSTEM = """Bạn là chuyên gia SQL cho DuckDB.

QUY TẮC BẮT BUỘC:
- Trả về MỘT câu SQL duy nhất. Không markdown. Không giải thích. Không ```.
- Chỉ dùng bảng/cột có trong SCHEMA. Không tự bịa.
- Tất cả cột revenue trong database đều là VND (đồng nguyên). 1 tỷ = 1,000,000,000 đồng.
- ROUND(revenue/1000000000.0, 2) để hiển thị đơn vị tỷ đồng nếu cần.
- LIMIT 200 nếu không yêu cầu cụ thể.
- "top N" → ORDER BY ... DESC LIMIT N.
- "tỷ lệ chuyển đổi" → ROUND(CAST(converted AS DOUBLE)/NULLIF(leads,0)*100, 2).
- Alias cột bằng tiếng Anh viết thường, không dấu."""

_SQL_FIX = "Debug SQL DuckDB. Trả về SQL đã sửa. Chỉ SQL, không markdown."

_SUMMARY_SYSTEM = """Bạn là chuyên gia phân tích kinh doanh BĐS Việt Nam.

NHIỆM VỤ: Tóm tắt kết quả truy vấn trong 2-3 câu. Tiếng Việt có dấu.

QUY TẮC:
1. CHỈ dùng số liệu từ kết quả SQL. Không thêm thông tin ngoài.
2. Revenue trong DB là VND. Khi tóm tắt: nếu giá trị >= 1,000,000,000 → dùng "tỷ đồng"; nếu >= 1,000,000 → "triệu đồng".
3. KHÔNG dùng: "có thể", "nhiều khi", "đa số", "thường thì", "có lẽ", "dường như".
4. Mỗi câu phải có con số cụ thể với đơn vị rõ ràng.
5. Kiểm tra: tổng phải lớn hơn thành phần, ngày cao nhất phải lớn hơn trung bình."""

_SMALLTALK_SYSTEM = """Bạn là trợ lý Agentic BI chuyên phân tích dữ liệu kinh doanh BĐS.
Trả lời ngắn gọn, thân thiện, tiếng Việt có dấu.
Nếu user hỏi bạn là gì: giải thích bạn là AI phân tích dữ liệu BĐS, có thể trả lời câu hỏi về doanh thu, sản phẩm, marketing, nhà môi giới.
Không bịa số liệu. Không tự chạy SQL."""

_FOLLOWUP_SYSTEM = """Bạn là chuyên gia tư vấn kinh doanh BĐS Việt Nam.
Dựa vào lịch sử hội thoại bên dưới, trả lời câu hỏi follow-up của user.

QUY TẮC:
1. Chỉ dùng số liệu đã có trong lịch sử. Không bịa thêm.
2. Nếu hỏi "nên làm gì/hành động/chiến lược" → đưa ra 2-3 khuyến nghị cụ thể từ data.
3. Nếu hỏi "ý nghĩa/giải thích" → phân tích ý nghĩa kinh doanh của con số đó.
4. Tiếng Việt có dấu. Ngắn gọn 3-5 câu.
5. KHÔNG dùng: "có thể", "nhiều khi", "có lẽ", "dường như"."""

_VAGUE = ["có thể","nhiều khi","đa số","thường thì","có lẽ","dường như","phần lớn","thông thường"]

def _clean(text: str) -> str:
    parts = re.split(r'(?<=[.!?])\s+', text.strip())
    out = [s for s in parts if not any(p in s.lower() for p in _VAGUE) or re.search(r'\d',s)]
    return " ".join(out) if out else text

def _fmt(v: float) -> str:
    if v >= 1e12: return f"{v/1e12:.2f} nghìn tỷ đồng"
    if v >= 1e9:  return f"{v/1e9:.2f} tỷ đồng"
    if v >= 1e6:  return f"{v/1e6:.0f} triệu đồng"
    return f"{v:,.0f} đồng"

def _auto_summary(df: pd.DataFrame) -> str:
    num_c = df.select_dtypes(include="number").columns.tolist()
    cat_c = df.select_dtypes(include="object").columns.tolist()
    parts = [f"Kết quả gồm {len(df)} dòng dữ liệu."]
    if num_c:
        col = num_c[0]; total = df[col].sum()
        parts.append(f"Tổng {col}: {_fmt(total) if total > 1e4 else f'{total:,.2f}'}.")
    if cat_c and num_c:
        try:
            top = df.loc[df[num_c[0]].idxmax()]
            val = top[num_c[0]]
            parts.append(f"Cao nhất: {top[cat_c[0]]} ({_fmt(val) if val > 1e4 else f'{val:,.2f}'}).")
        except Exception:
            pass
    return " ".join(parts)


# ================================================================
# MAIN AGENT LOOP
# ================================================================

def run_agent(question: str) -> Generator[AgentEvent, None, None]:

    # Step 1: Validate
    yield AgentEvent("validate", "[Kiểm tra] Đang xác thực câu hỏi...")
    v = validate_input(question)
    if not v.is_valid:
        yield AgentEvent("error", v.reason, {"type":"validation_error"})
        return

    history     = get_history()
    has_history = len(history) >= 2
    intent      = _classify_intent(question, has_history)

    push("user", question)

    # ---- SMALLTALK: trả lời trực tiếp, không SQL -------------------
    if intent == "smalltalk":
        yield AgentEvent("route", "[Nhận diện] Câu hỏi về trợ lý. Đang trả lời trực tiếp...")
        try:
            ans = _llm(_SMALLTALK_SYSTEM,
                       [{"role":"user","content":question}], 300)
        except Exception as e:
            ans = f"Tôi là trợ lý phân tích dữ liệu BĐS. Lỗi phản hồi: {e}"
        push("assistant", ans)
        yield AgentEvent("done","Hoàn thành.",
            {"df":pd.DataFrame(),"sql":"","summary":ans,"tables":[],"question":question})
        return

    # ---- FOLLOW-UP: dùng context có sẵn, không SQL mới ------------
    if intent == "followup":
        yield AgentEvent("route", "[Ngữ cảnh] Câu hỏi kế thừa kết quả trước. Đang phân tích...")
        try:
            ans = _llm(_FOLLOWUP_SYSTEM, history + [{"role":"user","content":question}], 500)
            ans = _clean(ans)
        except Exception as e:
            ans = f"Không thể phân tích ngữ cảnh: {e}"
        push("assistant", ans)
        yield AgentEvent("done","Hoàn thành.",
            {"df":pd.DataFrame(),"sql":"","summary":ans,"tables":[],"question":question})
        return

    # ---- DATA QUERY: cần SQL --------------------------------------
    yield AgentEvent("route", "[Định tuyến] Đang xác định bảng dữ liệu...")
    routing = route_question(question)
    if not routing.is_answerable:
        msg = ("Hệ thống chưa có dữ liệu để trả lời. "
               "Dữ liệu hiện có: doanh thu hàng ngày, hiệu suất sản phẩm, "
               "phễu marketing, hiệu suất nhà môi giới.")
        push("assistant", msg)
        yield AgentEvent("error", msg, {"type":"out_of_scope"})
        return

    yield AgentEvent("route", f"[Định tuyến] Bảng: {', '.join(routing.tables)}")
    schema_ctx = build_schema_context(routing.tables)

    yield AgentEvent("sql", "[Lập luận] Đang tạo câu lệnh SQL...")
    # Inject schema + history để SQL aware context
    sql_msgs = history + [{"role":"user",
        "content": f"SCHEMA:\n{schema_ctx}\n\nCÂU HỎI: {question}"}]
    try:
        sql = _llm(_SQL_SYSTEM, sql_msgs, 512)
    except Exception as e:
        yield AgentEvent("error", f"Lỗi tạo SQL: {e}", {"type":"api_error"})
        return

    yield AgentEvent("sql", "[Lập luận] SQL tạo xong.", {"sql":sql})

    df = pd.DataFrame(); current_sql = sql; last_err = ""

    for attempt in range(1, MAX_SQL_RETRIES + 1):
        lbl = "[Thực thi] Đang chạy truy vấn..." if attempt == 1 \
              else f"[Tự sửa lỗi] Lần {attempt}..."
        yield AgentEvent("execute", lbl)
        df, err = execute_query(current_sql)
        if err is None:
            break
        last_err = err
        if attempt < MAX_SQL_RETRIES:
            fix = f"SQL lỗi:\n{current_sql}\n\nLỗi:\n{err}\n\nSCHEMA:\n{schema_ctx}\n\nSQL đã sửa:"
            try:
                current_sql = _llm(_SQL_FIX,[{"role":"user","content":fix}],512)
            except Exception as e:
                yield AgentEvent("error",f"Lỗi sửa SQL: {e}",{"type":"api_error"})
                return
    else:
        yield AgentEvent("error",
            f"Không thể thực thi sau {MAX_SQL_RETRIES} lần. Lỗi: {last_err}",
            {"type":"sql_error","sql":current_sql})
        return

    if df.empty:
        msg = "Không tìm thấy dữ liệu phù hợp với điều kiện truy vấn."
        push("assistant", msg)
        yield AgentEvent("done","Hoàn thành.",
            {"df":df,"sql":current_sql,"summary":msg,"tables":routing.tables,"question":question})
        return

    yield AgentEvent("summarize","[Hoàn thành] Đang tổng hợp kết quả...")

    result_str = df.head(15).to_string(index=False)
    sum_msgs = [{"role":"user",
        "content":(f"Câu hỏi: {question}\n\n"
                   f"Kết quả ({len(df)} dòng, cột: {', '.join(df.columns)}):\n{result_str}\n\n"
                   f"Lưu ý: revenue là VND. Ví dụ 450000000 = 450 triệu đồng, 27000000000 = 27 tỷ đồng.\n"
                   f"Viết phân tích từ số liệu trên. Tiếng Việt có dấu.")}]
    try:
        raw = _llm(_SUMMARY_SYSTEM, sum_msgs, 400)
        summary = _clean(raw)
    except Exception:
        summary = _auto_summary(df)

    # Lưu summary vào history để follow-up sau dùng được
    push("assistant", f"[Kết quả cho '{question}']: {summary}\nData: {result_str[:500]}")

    yield AgentEvent("done","Hoàn thành phân tích.",
        {"df":df,"sql":current_sql,"summary":summary,"tables":routing.tables,"question":question})
