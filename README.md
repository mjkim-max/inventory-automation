# Inventory Automation

이지어드민/품고/쿠팡 재고를 수집해 Google Sheets에 기록하고, Streamlit 대시보드로 시각화합니다.

## 구성
- `scripts/ezadmin_stock_sync.py`: 재고 수집 및 시트 업데이트
- `app.py`: Streamlit 대시보드
- `.streamlit/secrets.toml`: 비밀키 (Git 제외)
- `.streamlit/secrets.example.toml`: 예시 템플릿

## 실행

```bash
pip install -r requirements.txt
python3 -m playwright install chromium
python3 /Users/kmj/Desktop/Cursor/inventory-automation/scripts/ezadmin_stock_sync.py
python3 -m streamlit run /Users/kmj/Desktop/Cursor/inventory-automation/app.py
```

## 시크릿 설정
`.streamlit/secrets.toml`에 인증값을 넣습니다.  
예시는 `.streamlit/secrets.example.toml` 참고.
