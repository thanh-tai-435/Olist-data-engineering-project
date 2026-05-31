# ============================================================
# validator.py v4
# Fix:
#   1. Thêm từ khóa phân tích ngữ cảnh: "ý nghĩa", "hành động",
#      "bài trên", "kết quả trên", "nên làm gì", "gợi ý"...
#   2. Bỏ cơ chế "bắt buộc phải có BI keyword" vì câu hỏi
#      follow-up hợp lệ thường không chứa từ khóa kỹ thuật.
#   3. Thay bằng logic 2 tầng: chặn rác → cho qua nếu không rác.
# ============================================================

import re
import unicodedata
from dataclasses import dataclass
from database import GOLD_SCHEMA

INVALID_MSG = (
    "Xin lỗi, tôi chỉ có thể trả lời các câu hỏi liên quan đến "
    "dữ liệu kinh doanh (doanh thu, sản phẩm, marketing, nhà môi giới). "
    "Vui lòng thử lại với câu hỏi phù hợp."
)

@dataclass
class ValidationResult:
    is_valid: bool
    reason: str

@dataclass
class RoutingResult:
    tables: list[str]
    is_answerable: bool
    confidence: str


def _no_accent(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


# ---- Tầng 1: Chặn cứng (nguy hiểm / rác rõ ràng) ---------------
_BLOCK_PATTERNS = [
    re.compile(r"https?://\S+"),
    re.compile(r"[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}"),
    re.compile(r"<[^>]+>"),
    re.compile(r"(ignore|forget|disregard).{0,30}(instruction|system|prompt)", re.I),
    re.compile(r"\b(DROP|DELETE|INSERT|UPDATE|ALTER|TRUNCATE)\s", re.I),
    re.compile(r"^[\W\d_]{0,5}$"),
]

_MIN_CHARS = 2
_MAX_CHARS = 600

# Câu chào hỏi xã giao thuần túy (match toàn bộ câu sau khi strip)
_GREETING_EXACT = {
    "hello", "hi", "hey", "xin chao", "xin chào", "chao", "chào",
    "alo", "yo", "ok", "okay", "oke", "noted",
    "cam on", "cảm ơn", "thank", "thanks",
    "lol", "haha", "hihi", "hehe",
    ":", "?", "!", "...",
}

# ---- Tầng 2: Chặn chủ đề hoàn toàn ngoài BI (không dấu) --------
_HARD_BLOCK_TOPICS = [
    # Công nghệ / AI chung
    "gpt la gi", "chatgpt la gi", "openai la gi", "llm la gi",
    "lap trinh web", "hoc python", "javascript tutorial",
    "hack ", "crack ", "virus ", "malware",
    # Đời sống cá nhân
    "thoi tiet hom nay", "weather hom nay",
    "bong da ", "ket qua bong", "ca nhac ", "phim chieu",
    "tinh yeu ", "ban gai ", "ban trai ", "ket hon ", "ly hon ",
    "bai tap hoc ", "lam bai thi",
    # Tài chính ngoài phạm vi
    "gia vang hom nay", "ti gia usd", "bitcoin hom nay",
    "lai suat ngan hang", "gui tiet kiem",
    # Kiến thức phổ thông
    "thu do nuoc ", "dan so the gioi", "lich su viet nam",
]

# ---- Tầng 3: Whitelist — câu hỏi dạng này LUÔN cho qua ----------
# Follow-up / phân tích ngữ cảnh từ kết quả trước
_CONTEXT_FOLLOWUP = [
    # Hỏi về kết quả vừa hiển thị
    "y nghia", "ý nghĩa", "bai tren", "bài trên", "ket qua tren", "kết quả trên",
    "hinh tren", "hình trên", "bieu do tren", "biểu đồ trên",
    "so lieu tren", "số liệu trên", "du lieu tren", "dữ liệu trên",
    "dieu nay", "điều này", "con so nay", "con số này",
    "thong tin tren", "thông tin trên",
    # Hỏi hành động / khuyến nghị
    "nen lam gi", "nên làm gì", "hanh dong gi", "hành động gì",
    "co the lam gi", "có thể làm gì", "nen tap trung", "nên tập trung",
    "khuyen nghi", "khuyến nghị", "goi y", "gợi ý",
    "cai thien", "cải thiện", "tang cuong", "tăng cường",
    "chien luoc", "chiến lược", "ke hoach", "kế hoạch",
    "de xuat", "đề xuất", "xu huong", "xu hướng",
    # So sánh / đánh giá
    "tot hay xau", "tốt hay xấu", "cao hay thap", "cao hay thấp",
    "co on khong", "có ổn không", "binh thuong khong", "bình thường không",
    "muc nay", "mức này", "nhu vay", "như vậy", "nhu the", "như thế",
    # Giải thích
    "tai sao", "tại sao", "ly do", "lý do", "nguyen nhan", "nguyên nhân",
    "giai thich", "giải thích", "co nghia la", "có nghĩa là",
    "tuc la", "tức là", "nghe co ve", "nghe có vẻ",
    # Câu hỏi tổng hợp
    "tong ket", "tổng kết", "tom tat", "tóm tắt", "ket luan", "kết luận",
    "nhin chung", "nhìn chung", "danh gia", "đánh giá",
    "phan tich them", "phân tích thêm", "chi tiet hon", "chi tiết hơn",
    "mo rong", "mở rộng",
]

# Từ khóa BI core — dùng để route, không dùng để chặn
_BI_KEYWORDS = [
    "doanh thu", "revenue", "ban hang", "bán hàng", "don hang", "đơn hàng",
    "doanh so", "doanh số", "kenh", "kênh", "online", "offline", "partner",
    "san pham", "sản phẩm", "product", "can ho", "căn hộ", "nha pho", "nhà phố",
    "biet thu", "biệt thự", "shophouse", "dat nen", "đất nền",
    "ban chay", "bán chạy", "hieu suat", "hiệu suất",
    "marketing", "pheu", "phễu", "funnel", "quang cao", "quảng cáo",
    "ads", "facebook", "google", "zalo", "lead", "tiem nang", "tiềm năng",
    "chuyen doi", "chuyển đổi", "ty le", "tỷ lệ",
    "nha moi gioi", "nhà môi giới", "moi gioi", "môi giới", "agent",
    "nhan vien", "nhân viên", "khu vuc", "khu vực",
    "dashboard", "bieu do", "biểu đồ", "chart", "bao cao", "báo cáo",
    "thong ke", "thống kê", "phan tich", "phân tích", "so sanh", "so sánh",
    "top", "hang", "hạng", "cao nhat", "cao nhất", "thap nhat", "thấp nhất",
    "tong", "tổng", "trung binh", "trung bình", "tuan", "tuần",
    "thang", "tháng", "quy", "quý", "ngay", "ngày",
    "loi nhuan", "lợi nhuận", "profit", "chi phi", "chi phí",
    "kpi", "tang", "tăng", "giam", "giảm",
]


def validate_input(text: str) -> ValidationResult:
    stripped = text.strip()

    if len(stripped) < _MIN_CHARS or len(stripped) > _MAX_CHARS:
        return ValidationResult(False, INVALID_MSG)

    # Tầng 1: block pattern nguy hiểm
    for p in _BLOCK_PATTERNS:
        if p.search(stripped):
            return ValidationResult(False, INVALID_MSG)

    lower    = stripped.lower()
    no_acc   = _no_accent(stripped)

    # Chặn câu chào ngắn thuần túy
    if lower in _GREETING_EXACT or no_acc in _GREETING_EXACT:
        return ValidationResult(False, INVALID_MSG)

    # Whitelist: câu follow-up phân tích ngữ cảnh → cho qua ngay
    for kw in _CONTEXT_FOLLOWUP:
        if kw in lower or kw in no_acc:
            return ValidationResult(True, "OK")

    # Whitelist: câu có từ khóa BI → cho qua
    for kw in _BI_KEYWORDS:
        if kw in lower or _no_accent(kw) in no_acc:
            return ValidationResult(True, "OK")

    # Tầng 2: chặn chủ đề ngoài BI rõ ràng
    for term in _HARD_BLOCK_TOPICS:
        if term in no_acc:
            return ValidationResult(False, INVALID_MSG)

    # Câu hỏi ngắn mơ hồ không rõ BI cũng không rõ off-topic
    # → cho qua, để agent tự xử lý
    if len(stripped.split()) <= 8:
        return ValidationResult(True, "OK")

    return ValidationResult(False, INVALID_MSG)


# ---- Routing --------------------------------------------------------

_ROUTING_KEYWORDS: dict[str, list[str]] = {
    "gold_daily_revenue": [
        "doanh thu", "revenue", "ban hang", "bán hàng", "don hang", "đơn hàng",
        "kenh ban", "kênh bán", "online", "offline", "partner",
        "doanh so", "doanh số", "ngay", "ngày", "thang", "tháng",
        "tuan", "tuần", "quy", "quý",
    ],
    "gold_product_performance": [
        "san pham", "sản phẩm", "product", "can ho", "căn hộ",
        "nha pho", "nhà phố", "biet thu", "biệt thự", "shophouse",
        "dat nen", "đất nền", "nghi duong", "nghỉ dưỡng",
        "ban chay", "bán chạy", "loai hinh", "loại hình",
        "so luong", "số lượng", "units",
    ],
    "gold_marketing_funnel": [
        "marketing", "pheu", "phễu", "funnel", "quang cao", "quảng cáo",
        "ads", "facebook", "google", "zalo", "lead",
        "tiem nang", "tiềm năng", "chuyen doi", "chuyển đổi",
        "impression", "ty le", "tỷ lệ",
    ],
    "gold_agent_performance": [
        "nha moi gioi", "nhà môi giới", "moi gioi", "môi giới",
        "agent", "nhan vien", "nhân viên", "khu vuc", "khu vực",
        "rating", "danh gia", "đánh giá", "chot", "chốt",
    ],
}

_OUT_OF_SCOPE = [
    "du bao", "dự báo", "forecast", "predict",
    "tuong lai", "tương lai", "nam sau", "năm sau",
]


def route_question(question: str) -> RoutingResult:
    lower  = question.lower()
    no_acc = _no_accent(question)

    for term in _OUT_OF_SCOPE:
        if term in lower or _no_accent(term) in no_acc:
            return RoutingResult([], False, "high")

    matched = []
    for table, keywords in _ROUTING_KEYWORDS.items():
        if any(kw in lower or _no_accent(kw) in no_acc for kw in keywords):
            matched.append(table)

    if not matched:
        return RoutingResult(list(GOLD_SCHEMA.keys()), True, "low")

    return RoutingResult(matched, True, "high" if len(matched) == 1 else "medium")


def build_schema_context(tables: list[str]) -> str:
    lines = ["=== GOLD LAYER SCHEMA (DuckDB) ===\n"]
    for table in tables:
        info = GOLD_SCHEMA.get(table, {})
        lines.append(f"Table: {table}")
        lines.append(f"  Mô tả: {info.get('description', '')}")
        for col, desc in info.get("columns", {}).items():
            lines.append(f"  - {col}: {desc}")
        lines.append("")
    return "\n".join(lines)