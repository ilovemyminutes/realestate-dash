"""
아파트별 뉴스 수집 및 LLM 기반 관련성 판단 스크립트

이 스크립트는:
1. BigQuery에서 아파트 목록을 조회
2. 네이버 검색 API로 각 아파트별 뉴스 수집
3. LLM(OpenAI/Claude)을 사용해 관련성 판단
4. 관련성 높은 뉴스만 필터링하여 JSON 저장

Usage:
    python workflow/collect_apartment_news.py

Environment Variables:
    - NAVER_CLIENT_ID: 네이버 API 클라이언트 ID
    - NAVER_CLIENT_SECRET: 네이버 API 시크릿
    - OPENAI_API_KEY: OpenAI API 키 (관련성 판단용)
    - GOOGLE_APPLICATION_CREDENTIALS: BigQuery 인증 (선택)
"""

import json
import os
import re
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime

# ============================================
# 설정
# ============================================

OUTPUT_PATH = "data/apartment_news.json"
NEWS_PER_APARTMENT = 10  # 아파트당 수집할 뉴스 수
MIN_RELEVANCE_SCORE = 0.6  # 관련성 점수 임계값 (0~1)

# 주요 아파트 목록 (BigQuery 조회 대신 직접 지정도 가능)
TARGET_APARTMENTS = [
    {"name": "헬리오시티", "region": "가락동"},
    {"name": "래미안블레스티지", "region": "개포동"},
    {"name": "래미안대치팰리스", "region": "대치동"},
    {"name": "리센츠", "region": "잠실동"},
    {"name": "반포자이", "region": "반포동"},
    {"name": "래미안원베일리", "region": "반포동"},
    {"name": "잠실엘스", "region": "잠실동"},
    {"name": "아크로리버파크", "region": "반포동"},
    {"name": "트리지움", "region": "잠실동"},
    {"name": "래미안퍼스티지", "region": "반포동"},
]


# ============================================
# 유틸리티 함수
# ============================================


def clean_html(text: str) -> str:
    """HTML 태그 및 특수문자 제거, JSON 안전 문자열로 변환"""
    clean = re.sub(r"<[^>]+>", "", text)
    clean = clean.replace("&quot;", '"')
    clean = clean.replace("&amp;", "&")
    clean = clean.replace("&lt;", "<")
    clean = clean.replace("&gt;", ">")
    clean = clean.replace("&apos;", "'")
    # JSON 문자열 내 큰따옴표 → 작은따옴표
    clean = re.sub(r'"([^"]+)"', r"'\1'", clean)
    clean = clean.replace('"', "'")
    return clean.strip()


def parse_date(pub_date_str: str) -> str:
    """날짜 문자열을 YYYY-MM-DD 형식으로 변환"""
    try:
        from email.utils import parsedate_to_datetime

        dt = parsedate_to_datetime(pub_date_str)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return pub_date_str[:10] if len(pub_date_str) >= 10 else pub_date_str


def extract_source(link: str) -> str:
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
        "etoday.co.kr": "이투데이",
        "newspim.com": "뉴스핌",
        "moneys.co.kr": "머니S",
        "bizhankook.com": "비즈한국",
    }
    for domain, source in source_mapping.items():
        if domain in link:
            return source
    return "기타"


# ============================================
# 네이버 뉴스 검색
# ============================================


def search_naver_news(query: str, display: int = 10) -> dict:
    """네이버 뉴스 검색 API 호출"""
    client_id = os.environ.get("NAVER_CLIENT_ID")
    client_secret = os.environ.get("NAVER_CLIENT_SECRET")

    if not client_id or not client_secret:
        print("⚠️ 네이버 API 키가 설정되지 않았습니다.")
        return {"items": [], "total": 0}

    encoded_query = urllib.parse.quote(query)
    url = f"https://openapi.naver.com/v1/search/news.json?query={encoded_query}&display={display}&sort=date"

    request = urllib.request.Request(url)
    request.add_header("X-Naver-Client-Id", client_id)
    request.add_header("X-Naver-Client-Secret", client_secret)

    try:
        response = urllib.request.urlopen(request)
        return json.loads(response.read().decode("utf-8"))
    except Exception as e:
        print(f"   ❌ 네이버 API 오류: {e}")
        return {"items": [], "total": 0}


# ============================================
# LLM 기반 관련성 판단
# ============================================


def judge_relevance_with_llm(apartment_name: str, news_title: str, news_desc: str) -> dict:
    """
    LLM을 사용하여 뉴스의 관련성을 판단

    Returns:
        dict: {"score": 0.0~1.0, "reason": "판단 근거"}
    """
    api_key = os.environ.get("OPENAI_API_KEY")

    if not api_key:
        # API 키가 없으면 키워드 기반 간단 판단
        return judge_relevance_simple(apartment_name, news_title, news_desc)

    try:
        import openai

        client = openai.OpenAI(api_key=api_key)

        prompt = f"""당신은 부동산 뉴스 분석 전문가입니다.

아래 뉴스가 '{apartment_name}' 아파트와 직접적으로 관련이 있는지 판단해주세요.

[뉴스 제목]
{news_title}

[뉴스 내용]
{news_desc}

판단 기준:
- 해당 아파트가 직접 언급되어 구체적인 정보(가격, 거래, 재건축, 분양 등)가 있으면 관련성 높음 (0.8~1.0)
- 해당 아파트가 언급되지만 단순 나열에 불과하면 중간 (0.5~0.7)
- 동일 지역이나 유사 아파트만 언급되면 낮음 (0.3~0.5)
- 전혀 관련 없으면 매우 낮음 (0~0.3)

JSON 형식으로 응답해주세요:
{{"score": 0.0~1.0, "reason": "판단 근거 한 줄 요약"}}"""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            max_tokens=150,
            temperature=0.1,
        )

        result = json.loads(response.choices[0].message.content)
        return result

    except Exception as e:
        print(f"   ⚠️ LLM 판단 실패, 키워드 기반으로 대체: {e}")
        return judge_relevance_simple(apartment_name, news_title, news_desc)


def judge_relevance_simple(apartment_name: str, news_title: str, news_desc: str) -> dict:
    """
    키워드 기반 간단 관련성 판단 (LLM 없이)
    """
    combined = f"{news_title} {news_desc}".lower()
    apt_lower = apartment_name.lower()

    # 직접 언급 체크
    if apt_lower in combined:
        # 구체적 정보 체크 (가격, 거래 등)
        price_keywords = ["억", "만원", "거래", "신고가", "최고가", "매매", "전세"]
        has_price_info = any(kw in combined for kw in price_keywords)

        if has_price_info:
            return {"score": 0.9, "reason": "직접 언급 + 가격/거래 정보"}
        else:
            return {"score": 0.7, "reason": "직접 언급"}

    # 단지명 일부 매칭
    apt_parts = apt_lower.replace("래미안", "").replace("자이", "").replace("힐스테이트", "")
    if len(apt_parts) > 2 and apt_parts in combined:
        return {"score": 0.5, "reason": "부분 매칭"}

    return {"score": 0.2, "reason": "관련성 낮음"}


# ============================================
# 메인 로직
# ============================================


@dataclass
class NewsItem:
    title: str
    link: str
    description: str
    pubDate: str
    source: str
    relevance: str
    relevance_score: float


def collect_apartment_news(apartment: dict, use_llm: bool = True) -> dict:
    """단일 아파트에 대한 뉴스 수집 및 필터링"""
    apt_name = apartment["name"]
    region = apartment["region"]

    print(f"\n🔍 [{apt_name}] ({region}) 뉴스 수집 중...")

    # 뉴스 검색
    search_result = search_naver_news(f"{apt_name} 아파트", NEWS_PER_APARTMENT)

    if not search_result.get("items"):
        print("   ⚠️ 검색 결과 없음, 지역명으로 재검색...")
        search_result = search_naver_news(f"{region} {apt_name}", NEWS_PER_APARTMENT)

    total_news = search_result.get("total", 0)
    raw_items = search_result.get("items", [])

    print(f"   📰 총 {total_news:,}건 중 {len(raw_items)}건 분석...")

    # 관련성 판단 및 필터링
    filtered_items = []

    for item in raw_items:
        title = clean_html(item.get("title", ""))
        desc = clean_html(item.get("description", ""))

        # 관련성 판단
        if use_llm:
            relevance = judge_relevance_with_llm(apt_name, title, desc)
        else:
            relevance = judge_relevance_simple(apt_name, title, desc)

        score = relevance.get("score", 0)
        reason = relevance.get("reason", "")

        if score >= MIN_RELEVANCE_SCORE:
            news_item = NewsItem(
                title=title,
                link=item.get("link", ""),
                description=desc,
                pubDate=parse_date(item.get("pubDate", "")),
                source=extract_source(item.get("originallink", item.get("link", ""))),
                relevance=reason,
                relevance_score=score,
            )
            filtered_items.append(asdict(news_item))
            print(f"   ✅ [{score:.1f}] {title[:40]}...")
        else:
            print(f"   ❌ [{score:.1f}] {title[:40]}... (필터링)")

    # 관련성 점수순 정렬
    filtered_items.sort(key=lambda x: x["relevance_score"], reverse=True)

    # relevance_score 필드 제거 (출력용)
    for item in filtered_items:
        del item["relevance_score"]

    # 요약 생성
    summary = generate_summary(apt_name, filtered_items)

    # 관련도 등급 산정
    avg_score = (
        sum(judge_relevance_simple(apt_name, item["title"], item["description"])["score"] for item in filtered_items)
        / len(filtered_items)
        if filtered_items
        else 0
    )
    relevance_level = "very_high" if avg_score >= 0.85 else "high" if avg_score >= 0.7 else "medium"

    return {
        "region": region,
        "total_news": total_news,
        "relevance_score": relevance_level,
        "news_count": len(filtered_items),
        "summary": summary,
        "items": filtered_items,
    }


def generate_summary(apt_name: str, items: list) -> str:
    """뉴스 아이템들로부터 요약 생성"""
    if not items:
        return f"{apt_name} 관련 최신 뉴스가 충분하지 않습니다."

    # 키워드 추출
    keywords = []
    all_text = " ".join([item["title"] + " " + item["description"] for item in items])

    if any(kw in all_text for kw in ["신고가", "최고가", "억원"]):
        keywords.append("신고가 경신")
    if any(kw in all_text for kw in ["상승", "급등", "오르"]):
        keywords.append("가격 상승")
    if any(kw in all_text for kw in ["재건축", "재개발", "정비"]):
        keywords.append("재건축")
    if any(kw in all_text for kw in ["분양", "청약", "입주"]):
        keywords.append("분양/입주")
    if any(kw in all_text for kw in ["거래량", "매물"]):
        keywords.append("거래 동향")

    if keywords:
        return f"{apt_name}: {', '.join(keywords[:3])} 관련 뉴스가 주목받고 있습니다."
    else:
        return f"{apt_name} 관련 최신 부동산 뉴스입니다."


def main():
    """메인 실행 함수"""
    print("=" * 60)
    print("🏢 아파트별 뉴스 수집 (LLM 관련성 판단)")
    print("=" * 60)

    # LLM 사용 여부 확인
    use_llm = bool(os.environ.get("OPENAI_API_KEY"))
    if use_llm:
        print("✅ OpenAI API 키 감지됨 - LLM 기반 관련성 판단 활성화")
    else:
        print("⚠️ OpenAI API 키 없음 - 키워드 기반 판단 사용")

    # 결과 수집
    result = {
        "last_updated": datetime.now().isoformat(),
        "metadata": {
            "total_apartments": len(TARGET_APARTMENTS),
            "relevance_filter": "LLM-based" if use_llm else "keyword-based",
            "min_relevance_score": MIN_RELEVANCE_SCORE,
            "description": "아파트별 뉴스 (관련성 높은 뉴스만 필터링)",
        },
        "apartments": {},
        "fallback_regions": {
            "description": "아파트별 뉴스가 부족한 경우 동 단위 뉴스 표시",
            "regions": list(set(apt["region"] for apt in TARGET_APARTMENTS)),
        },
    }

    for apartment in TARGET_APARTMENTS:
        apt_data = collect_apartment_news(apartment, use_llm=use_llm)
        result["apartments"][apartment["name"]] = apt_data

    # 저장
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 60)
    print(f"✅ 저장 완료: {OUTPUT_PATH}")
    print(f"📊 총 {len(result['apartments'])}개 아파트 수집")

    # 통계 출력
    total_news = sum(apt["news_count"] for apt in result["apartments"].values())
    print(f"📰 관련성 높은 뉴스 총 {total_news}건 수집")


if __name__ == "__main__":
    main()
