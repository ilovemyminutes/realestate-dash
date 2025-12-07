"""
네이버 뉴스 수집 스크립트

이 스크립트는 네이버 검색 API를 통해 지역별 부동산 뉴스를 수집하고
JSON 파일로 저장합니다.

Usage:
    python workflow/collect_news.py

Note:
    - 네이버 API 키가 필요합니다 (NAVER_CLIENT_ID, NAVER_CLIENT_SECRET 환경변수)
    - MCP 서버를 통해 수집하는 경우 이 스크립트 대신 MCP 도구를 직접 사용하세요.
"""

import json
import os
import re
from datetime import datetime

# 지역 목록 (BigQuery에서 조회한 주요 지역들)
TARGET_REGIONS = [
    "잠실동",
    "개포동",
    "대치동",
    "반포동",
    "서초동",
    "당산동",
    "여의도동",
    "목동",
    "봉천동",
    "신림동",
    "이촌동",
    "영등포동",
]

# 출력 파일 경로
OUTPUT_PATH = "data/news_headlines.json"


def clean_html_tags(text: str) -> str:
    """HTML 태그 및 특수문자 제거, JSON 안전 문자열로 변환"""
    # HTML 태그 제거
    clean = re.sub(r"<[^>]+>", "", text)
    # &quot; 등 HTML 엔티티 변환
    clean = clean.replace("&quot;", '"')
    clean = clean.replace("&amp;", "&")
    clean = clean.replace("&lt;", "<")
    clean = clean.replace("&gt;", ">")
    clean = clean.replace("&apos;", "'")

    # JSON 문자열 내 큰따옴표 처리 (겹따옴표 → 작은따옴표)
    # 예: "결국은 집값 오를 것" → '결국은 집값 오를 것'
    clean = re.sub(r'"([^"]+)"', r"'\1'", clean)

    # 혹시 남아있는 단독 큰따옴표도 작은따옴표로 변환
    clean = clean.replace('"', "'")

    return clean.strip()


def extract_source_from_link(link: str) -> str:
    """링크에서 뉴스 소스 추출"""
    source_mapping = {
        "hankyung.com": "한국경제",
        "sedaily.com": "서울경제",
        "newsis.com": "뉴시스",
        "fnnews.com": "파이낸셜뉴스",
        "mk.co.kr": "매일경제",
        "chosun.com": "조선일보",
        "donga.com": "동아일보",
        "joongang.co.kr": "중앙일보",
        "hani.co.kr": "한겨레",
        "khan.co.kr": "경향신문",
        "yna.co.kr": "연합뉴스",
        "sbs.co.kr": "SBS",
        "kbs.co.kr": "KBS",
        "mbc.co.kr": "MBC",
        "ichannela.com": "채널A",
        "inews24.com": "아이뉴스24",
        "newspim.com": "뉴스핌",
        "moneys.co.kr": "머니S",
        "pinpointnews.co.kr": "핀포인트뉴스",
        "areyou.co.kr": "아유경제",
        "munhwa.com": "문화일보",
        "mediapen.com": "미디어펜",
    }

    for domain, source in source_mapping.items():
        if domain in link:
            return source
    return "기타"


def format_pub_date(pub_date_str: str) -> str:
    """날짜 문자열을 YYYY-MM-DD 형식으로 변환"""
    try:
        # "Sun, 07 Dec 2025 19:32:00 +0900" 형식
        from email.utils import parsedate_to_datetime

        dt = parsedate_to_datetime(pub_date_str)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return pub_date_str[:10] if len(pub_date_str) >= 10 else pub_date_str


def create_sample_news_structure() -> dict:
    """
    샘플 뉴스 데이터 구조 생성

    실제로는 네이버 API나 MCP를 통해 수집한 데이터로 채워야 합니다.
    """
    return {"last_updated": datetime.now().isoformat(), "regions": {}}


def generate_summary(items: list, region: str) -> str:
    """뉴스 아이템들을 기반으로 요약 생성 (간단한 키워드 추출)"""
    if not items:
        return f"{region} 관련 최신 뉴스가 없습니다."

    # 키워드 추출 (제목에서)
    keywords = []
    for item in items[:3]:
        title = item.get("title", "")
        # 가격 관련
        if any(kw in title for kw in ["억", "상승", "오르", "급등"]):
            keywords.append("가격 상승")
        if any(kw in title for kw in ["하락", "떨어", "급락"]):
            keywords.append("가격 하락")
        # 재건축 관련
        if any(kw in title for kw in ["재건축", "재개발", "정비"]):
            keywords.append("재건축")
        # 분양 관련
        if any(kw in title for kw in ["분양", "청약", "입주"]):
            keywords.append("분양/입주")

    keywords = list(set(keywords))
    if keywords:
        return f"{region}은(는) {', '.join(keywords)} 관련 뉴스가 주목받고 있습니다."
    else:
        return f"{region} 관련 최신 부동산 뉴스입니다."


def process_news_item(item: dict) -> dict:
    """네이버 API 응답 아이템을 정제된 형식으로 변환"""
    return {
        "title": clean_html_tags(item.get("title", "")),
        "link": item.get("link", ""),
        "description": clean_html_tags(item.get("description", "")),
        "pubDate": format_pub_date(item.get("pubDate", "")),
        "source": extract_source_from_link(item.get("originallink", item.get("link", ""))),
    }


def main():
    """
    메인 실행 함수

    Note: 실제 API 호출은 네이버 개발자 센터에서 발급받은 API 키가 필요합니다.
    MCP 서버를 사용하는 경우 이 스크립트 대신 MCP 도구를 직접 호출하세요.
    """
    print("=" * 50)
    print("📰 네이버 뉴스 수집 스크립트")
    print("=" * 50)

    # 기존 데이터 로드 (있으면)
    existing_data = None
    if os.path.exists(OUTPUT_PATH):
        try:
            with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
            print(f"✅ 기존 데이터 로드: {OUTPUT_PATH}")
        except Exception as e:
            print(f"⚠️ 기존 데이터 로드 실패: {e}")

    # 환경변수에서 API 키 확인
    client_id = os.environ.get("NAVER_CLIENT_ID")
    client_secret = os.environ.get("NAVER_CLIENT_SECRET")

    if not client_id or not client_secret:
        print("\n⚠️ 네이버 API 키가 설정되지 않았습니다.")
        print("   환경변수를 설정하거나 MCP 도구를 사용하세요:")
        print("   - NAVER_CLIENT_ID")
        print("   - NAVER_CLIENT_SECRET")
        print("\n📌 현재는 기존 데이터를 유지하거나 MCP를 통해 수집하세요.")
        return

    # API 호출 (실제 구현)
    import urllib.parse
    import urllib.request

    news_data = {"last_updated": datetime.now().isoformat(), "regions": {}}

    for region in TARGET_REGIONS:
        print(f"\n🔍 {region} 뉴스 수집 중...")

        query = urllib.parse.quote(f"{region} 아파트")
        url = f"https://openapi.naver.com/v1/search/news.json?query={query}&display=5&sort=date"

        request = urllib.request.Request(url)
        request.add_header("X-Naver-Client-Id", client_id)
        request.add_header("X-Naver-Client-Secret", client_secret)

        try:
            response = urllib.request.urlopen(request)
            result = json.loads(response.read().decode("utf-8"))

            items = [process_news_item(item) for item in result.get("items", [])]

            news_data["regions"][region] = {
                "total_news": result.get("total", 0),
                "items": items,
                "summary": generate_summary(items, region),
            }

            print(f"   ✅ {len(items)}건 수집 완료")

        except Exception as e:
            print(f"   ❌ 오류 발생: {e}")
            # 기존 데이터 유지
            if existing_data and region in existing_data.get("regions", {}):
                news_data["regions"][region] = existing_data["regions"][region]
                print("   📦 기존 데이터 유지")

    # 결과 저장
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(news_data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 저장 완료: {OUTPUT_PATH}")
    print(f"📊 총 {len(news_data['regions'])}개 지역 수집")


if __name__ == "__main__":
    main()
