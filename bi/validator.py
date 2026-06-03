"""
3-tier input validator + intent classifier for Olist Agentic BI.

Tier 1 — Hard block  : SQL injection, URLs, pure greetings
Tier 2 — Off-topic   : weather, sports, entertainment, crypto, etc.
Tier 3 — BI whitelist: Olist domain keywords + context-aware follow-up signals
"""
import re

# ── Tier 1: hard block patterns ───────────────────────────────────────────────

_INJECTION = re.compile(
    r"\b(drop|delete|insert|update|create|alter|truncate|grant|revoke|exec|execute)\b",
    re.IGNORECASE,
)
_URL       = re.compile(r"https?://|www\.", re.IGNORECASE)
_EMAIL     = re.compile(r"\S+@\S+\.\S+")

_PURE_GREETINGS = re.compile(
    r"^(xin\s+ch[àa]o|hello|hi|hey|ch[àa]o\s*(b[ạa]n)?|good\s*(morning|afternoon|evening)|"
    r"c[aả]m\s*[oơ]n|thanks?|tks|ok\s*b[ạa]n?|bye|t[aạ]m\s*bi[eệ]t)[\s!.?]*$",
    re.IGNORECASE,
)

_SYSTEM_QUESTIONS = re.compile(
    r"b[aạ]n\s+(l[àa]\s+ai|l[àa]m\s+g[iì]|bi[eế]t|c[oó]\s+th[eể]|gi[uú]p)|"
    r"(h[uướ][oơ]ng\s+d[aẫ]n|gi[oớ]i\s+thi[eệ]u\s+b[aả]n\s+th[aâ]n|"
    r"b[aạ]n\s+t[eê]n\s+g[iì]|you\s+are|who\s+are\s+you|what\s+can\s+you)",
    re.IGNORECASE,
)

# ── Tier 2: off-topic blocklist ───────────────────────────────────────────────

_OFFTOPIC = re.compile(
    r"\b(th[oờ]i\s*ti[eế]t|nhi[eệ]t\s*[dđ][oộ]|m[uư][aả]|n[aắ]ng|"      # weather
    r"b[oó]ng\s*[dđ][aá]|th[eể]\s*thao|gi[aả]i\s*[vvV][[oô]]|"             # sports
    r"[aâ]m\s*nh[aạ]c|ca\s*nh[aạ]c|phim|game|gi[aả]i\s*tr[iíì]|"          # entertainment
    r"bitcoin|crypto|nft|ti[eề]n\s*[aả]o|ethereum|"                          # crypto
    r"n[aâ]u\s*[aă]n|c[oô]ng\s*th[uứ]c|m[oó]n\s*[aă]n|"                  # cooking
    r"y[eê]u|h[eẹ]n\s*h[oò]|t[iì]nh\s*y[eê]u|h[oô]n\s*nh[aâ]n|"         # romance
    r"ch[iíì]nh\s*tr[iị]|[bầ]u\s*c[uử]|[đd][aả]ng\s*ph[aá]i)\b",         # politics
    re.IGNORECASE,
)

# ── Tier 3: BI keyword whitelist (Olist domain) ───────────────────────────────

_BI_KEYWORDS = re.compile(
    r"\b("
    # orders / sales
    r"[đd][oơ]n\s*h[aà]ng|order|doanh\s*thu|revenue|b[aá]n\s*h[aà]ng|sales|"
    r"payment|thanh\s*to[aá]n|h[oó]a\s*[dđ][oơ]n|invoice|"
    # delivery
    r"giao\s*h[aà]ng|v[aậ]n\s*chuy[eể]n|delivery|freight|tr[eễ]|late|"
    r"th[oờ]i\s*gian\s*giao|[sS]LA|"
    # customers
    r"kh[aá]ch\s*h[aà]ng|customer|churn|mua\s*l[aạ]i|repeat|"
    # sellers / products
    r"seller|ng[uư][oờ]i\s*b[aá]n|s[aả]n\s*ph[aẩ]m|product|category|danh\s*m[uụ]c|"
    # marketing / funnel
    r"lead|funnel|chuy[eể]n\s*[đd][oổ]i|conversion|marketing|origin|"
    # analytics
    r"ph[aâ]n\s*t[iích]ch|top|b[aá]o\s*c[aá]o|th[oố]ng\s*k[eê]|so\s*s[aá]nh|"
    r"t[oổ]ng|trung\s*b[iì]nh|average|mean|sum|count|"
    # time
    r"th[aá]ng|n[aă]m|ng[aà]y|month|year|day|qu[yý]|quarter|2017|2018|"
    # geo / Brazil
    r"bang|state|city|th[aà]nh\s*ph[oố]|SP|RJ|MG|Brazil|"
    # review / quality
    r"review|[đd][aá]nh\s*gi[aá]|score|rating|ch[aấ]t\s*l[uư][oợ]ng|"
    # data
    r"data|d[uữ]\s*li[eệ]u|b[aả]ng|table|query|sql"
    r")\b",
    re.IGNORECASE,
)

# ── Follow-up signals ─────────────────────────────────────────────────────────

_FOLLOWUP_PREFIX = re.compile(
    r"^(v[aậ]y|c[oò]n|th[eế]\s*th[iì]|v[aậ]y\s*th[iì]|th[eế]|"
    r"t[aạ]i\s*sao|v[iì]\s*sao|[ýý]\s*ngh[iĩ]a|gi[aả]i\s*th[iích]ch|"
    r"ph[aâ]n\s*t[iích]ch\s*th[eê]m|c[uụ]\s*th[eể]\s*h[oơ]n|"
    r"breakdown|drill|so\s*s[aá]nh\s*th[eê]m|k[eế]t\s*qu[aả]\s*n[aà]y|"
    r"[đd]i[eề]u\s*n[aà]y|n[oó]\s+c[oó])",
    re.IGNORECASE,
)

_FOLLOWUP_REF = re.compile(
    r"\b(k[eế]t\s*qu[aả]\s*n[aà]y|[đd]i[eề]u\s*n[aà]y|c[aá]c\s*s[oố]\s*n[aà]y|"
    r"nh[uư]\s*v[aậ]y|n[oó]\s*c[oó]|b[aả]ng\s*tr[eê]n|nh[uư]\s*tr[eê]n)\b",
    re.IGNORECASE,
)


# ── Public API ────────────────────────────────────────────────────────────────

def validate(text: str) -> tuple[bool, str]:
    """
    Returns (is_valid, reason).
    Invalid inputs are rejected with a user-friendly reason.
    """
    t = text.strip()
    if not t:
        return False, "Câu hỏi trống."
    if len(t) > 500:
        return False, "Câu hỏi quá dài (tối đa 500 ký tự)."
    if _INJECTION.search(t):
        return False, "Câu hỏi chứa lệnh SQL không hợp lệ."
    if _URL.search(t):
        return False, "Vui lòng không nhập URL."
    if _EMAIL.search(t):
        return False, "Vui lòng không nhập địa chỉ email."
    if _OFFTOPIC.search(t):
        return False, "Tôi chỉ trả lời câu hỏi về dữ liệu Olist (đơn hàng, doanh thu, khách hàng, sellers)."
    return True, ""


def classify_intent(text: str, history: list) -> str:
    """
    Returns one of: 'SMALLTALK' | 'FOLLOWUP' | 'DATA_QUERY'

    Rules (in order):
      1. Pure greeting / system question → SMALLTALK
      2. Followup signals + non-empty history + no strong BI keyword → FOLLOWUP
      3. Everything else → DATA_QUERY
    """
    t = text.strip()

    # Rule 1 — SMALLTALK
    if _PURE_GREETINGS.match(t) or _SYSTEM_QUESTIONS.search(t):
        return "SMALLTALK"

    # Rule 2 — FOLLOWUP
    has_followup_signal = bool(_FOLLOWUP_PREFIX.match(t) or _FOLLOWUP_REF.search(t))
    has_bi_keyword      = bool(_BI_KEYWORDS.search(t))
    is_short            = len(t.split()) < 12

    if history and has_followup_signal and (is_short or not has_bi_keyword):
        return "FOLLOWUP"

    # Rule 3 — DATA_QUERY
    return "DATA_QUERY"
