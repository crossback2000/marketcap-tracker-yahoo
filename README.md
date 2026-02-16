# Market Cap Tracker (초보자용 가이드)

미국 주식 시가총액 순위 변화를 웹 화면으로 보는 프로젝트입니다.  
처음 개발을 해보는 분도 따라 할 수 있게, 설치부터 실행까지 순서대로 정리했습니다.

## 1. 이 프로젝트로 할 수 있는 것
- 미국 시가총액 상위 260개 종목 순위 흐름 보기
- 종목별 과거 순위/시가총액 추이 보기
- 신규 진입/급상승 이벤트 확인
- 한국어 회사명 표시

## 2. 준비물
- Linux/macOS 터미널(또는 WSL)
- Python 3.10 이상
- 인터넷 연결 (Yahoo/네이버 데이터 수집용)

## 3. 처음 실행하기 (Step-by-step)
### 3-1. 가상환경 만들기
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3-2. 필요한 패키지 설치
```bash
pip install -r requirements.txt
cp .env.example .env
```

### 3-3. 데이터 수집 (처음 1회)
빠르게 테스트하려면 1년치:
```bash
python backfill_history.py --days 365 --universe-size 260 --store-limit 260
```

실사용 권장(15년치, 시간이 더 걸림):
```bash
python backfill_history.py --days 5475 --universe-size 260 --store-limit 260
```

### 3-4. 서버 실행
```bash
uvicorn app:app --host 0.0.0.0 --port 8010 --no-proxy-headers
```

브라우저에서 접속:
- `http://127.0.0.1:8010`

## 4. 이후 데이터 업데이트 방법
- 최근 구간만 갱신:
```bash
python fetch_and_store.py --universe-size 260 --store-limit 260 --days 30
```
- 한국어 회사명 월 1회 갱신:
```bash
python scripts/fetch_naver_usa_company_names_ko.py --limit 300
```

## 5. 자주 생기는 문제 해결
- `Database not found`  
  먼저 데이터 수집 명령(`backfill_history.py`)을 실행하세요.
- `Address already in use`  
  포트가 겹친 상태입니다. `--port 8011`처럼 다른 포트로 실행하세요.
- 패키지 import 오류  
  가상환경 활성화(`source .venv/bin/activate`) 후 다시 실행하세요.

## 6. 주요 파일 설명
- `app.py`: 웹 API + 화면 파일 제공
- `fetch_and_store.py`: Yahoo 데이터 수집/시총 계산/DB 저장
- `backfill_history.py`: 장기 데이터 백필 실행
- `scripts/fetch_naver_usa_company_names_ko.py`: 네이버 한국어명 수집
- `data/marketcap.db`: 저장된 시가총액/순위 DB

## 7. 참고/주의
- 시가총액은 기본적으로 `Close × Shares Outstanding`으로 계산합니다.
- Yahoo 데이터 특성상 일부 날짜/종목 결측이 있을 수 있습니다.
- 투자 판단 용도가 아닌 연구/학습용으로 사용하세요.

## 8. Docker로 실행하기
### 8-1. 이미지 빌드
```bash
docker compose build
```

### 8-2. API 서버 실행
```bash
docker compose up -d app
```

접속:
- `http://127.0.0.1:8010`

정지:
```bash
docker compose down
```

### 8-3. 데이터 백필/업데이트 실행 (일회성 job)
최초 백필(예: 1년):
```bash
docker compose run --rm job python backfill_history.py --days 365 --universe-size 260 --store-limit 260
```

최근 구간 업데이트(예: 30일):
```bash
docker compose run --rm job python fetch_and_store.py --universe-size 260 --store-limit 260 --days 30
```

드라이런:
```bash
docker compose run --rm job python fetch_and_store.py --dry-run --universe-size 260 --symbols-limit 40 --days 180
```

### 8-4. 데이터 파일 위치
- 호스트의 `./data`가 컨테이너 `/app/data`에 마운트됩니다.
- 따라서 `marketcap.db`, `company_names_ko.json`은 컨테이너 재생성 후에도 유지됩니다.
