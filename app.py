import streamlit as st

st.set_page_config(
    page_title="부동산 인사이트 대시보드", page_icon="🏠", layout="wide", initial_sidebar_state="expanded"
)

# --- 홈 페이지 ---
st.title("🏠 부동산 인사이트 대시보드")
st.markdown("---")

st.markdown(
    """
### 👋 환영합니다!

이 대시보드는 **빅쿼리(BigQuery) 실거래 데이터**와 **네이버 검색 MCP**를 결합하여
부동산 시장의 핵심 인사이트를 제공합니다.

---

### 📊 주요 기능

왼쪽 사이드바에서 원하는 분석 페이지를 선택하세요.
"""
)

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(
        """
    #### 📈 전세가율 분석
    - 아파트별 전세가율 현황
    - 동(지역)별 전세가율 비교
    - 갭투자 유망 단지 탐색
    - 깡통전세 위험 경고
    """
    )

with col2:
    st.markdown(
        """
    #### 📉 매매/전세 추이
    - 아파트별 시세 변동 추이
    - 동(지역)별 평균가 흐름
    - 실거래가 vs KB시세 비교
    - 신고가 갱신 모니터링
    """
    )

with col3:
    st.markdown(
        """
    #### 📰 부동산 뉴스
    - 아파트별 맞춤 뉴스
    - 지역별 부동산 이슈
    - 재개발/재건축 소식
    - 정책 변화 트래킹
    """
    )

st.markdown("---")

# 데이터 현황 요약
st.markdown("### 🗄️ 데이터 현황")

try:
    from utils.bq_client import FILTER_EXCLUDE_JUSANGBOKHAP, TABLE_COMPLEX, TABLE_JEONSAE, TABLE_MAEMAE, get_bq_client

    client = get_bq_client()

    stats_query = f"""
    SELECT
        (
            SELECT COUNT(*)
            FROM `{TABLE_MAEMAE}`
            WHERE price IS NOT NULL AND {FILTER_EXCLUDE_JUSANGBOKHAP}
        ) as maemae_count,
        (
            SELECT COUNT(*)
            FROM `{TABLE_JEONSAE}`
            WHERE price IS NOT NULL AND {FILTER_EXCLUDE_JUSANGBOKHAP}
        ) as jeonsae_count,
        (
            SELECT COUNT(DISTINCT apartment_name)
            FROM `{TABLE_COMPLEX}`
            WHERE {FILTER_EXCLUDE_JUSANGBOKHAP}
        ) as complex_count,
        (
            SELECT COUNT(DISTINCT region)
            FROM `{TABLE_COMPLEX}`
            WHERE {FILTER_EXCLUDE_JUSANGBOKHAP}
        ) as region_count
    """

    stats = client.query(stats_query).to_dataframe().iloc[0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("매매 실거래", f"{stats['maemae_count']:,}건")
    col2.metric("전세 실거래", f"{stats['jeonsae_count']:,}건")
    col3.metric("아파트 단지", f"{stats['complex_count']:,}개")
    col4.metric("분석 지역(동)", f"{stats['region_count']:,}개")

except Exception as e:
    st.warning(f"⚠️ 데이터 현황을 불러올 수 없습니다: {e}")

st.markdown("---")
st.caption("🔄 데이터는 매일 업데이트됩니다. | Made with Streamlit & BigQuery")
