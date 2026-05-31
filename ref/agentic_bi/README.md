# Agentic BI

He thong phan tich kinh doanh tu van hanh (Agentic Business Intelligence)  
xay dung tren Python, Streamlit, DuckDB va Groq LLM.

---

## Cau truc du an

```
agentic_bi/
  app.py            - Giao dien Streamlit (UI only, khong chua logic)
  agent.py          - Vong lap suy luan cua AI Agent
  database.py       - Ket noi DuckDB va du lieu Gold layer
  validator.py      - Kiem tra dau vao va dinh tuyen ngu nghia
  config.py         - Tai bien moi truong, kiem tra khi khoi dong
  requirements.txt  - Thu vien Python
  Dockerfile        - Dong goi container
  docker-compose.yml
  .env.example      - Mau file cau hinh (copy thanh .env)
  data/             - Thu muc luu file DuckDB (tu dong tao)
```

---

## Cai dat va chay

### Option 1: Chay truc tiep (local)

```bash
# 1. Tao moi truong ao
python -m venv venv
source venv/bin/activate        # Linux/Mac
# hoac: venv\Scripts\activate   # Windows

# 2. Cai thu vien
pip install -r requirements.txt

# 3. Tao file .env tu mau
cp .env.example .env
# Dien GROQ_API_KEY vao file .env

# 4. Chay ung dung
streamlit run app.py
# Truy cap: http://localhost:8501
```

### Option 2: Docker (khuyen dung cho VSCode Remote SSH)

```bash
# 1. Tao file .env (xem buoc 3 o tren)

# 2. Build va chay container
docker-compose up --build

# 3. Tren VSCode Remote SSH: Forward port 8501
# Truy cap: http://localhost:8501
```

---

## Gold Tables co san

| Bang                      | Mo ta                                      |
|---------------------------|--------------------------------------------|
| gold_daily_revenue        | Doanh thu hang ngay theo kenh ban hang     |
| gold_product_performance  | Hieu suat tung san pham bat dong san       |
| gold_marketing_funnel     | Pheu marketing theo kenh quang cao         |
| gold_agent_performance    | Hieu suat cua tung nha moi gioi            |

---

## Vi du cau hoi

- San pham nao ban chay nhat va doanh thu la bao nhieu?
- Tong doanh thu theo kenh Online trong 30 ngay?
- Nha moi gioi nao co ti le chot deal cao nhat?
- Kenh quang cao nao mang lai nhieu lead nhat?
- So sanh ti le chuyen doi giua Facebook Ads va Google Ads?

---

## Lay Groq API Key

Truy cap https://console.groq.com -> API Keys -> Create API Key  
Dan vao file .env: GROQ_API_KEY=gsk_...
