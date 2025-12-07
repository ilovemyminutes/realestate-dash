"""
📰 부동산 뉴스 페이지
- 아파트별 맞춤 뉴스
- 지역별 부동산 이슈
"""

import json
import sys

import pandas as pd
import streamlit as st

sys.path.append(".")

st.set_page_config(page_title="부동산 뉴스", page_icon="📰", layout="wide")

st.title("📰 부동산 뉴스")
st.markdown("**아파트별/지역별** 맞춤 부동산 뉴스를 확인하세요.")
st.markdown("---")


# --- 데이터 로딩 ---
@st.cache_data(ttl=1800)
def load_apartment_news():
    """아파트별 뉴스 데이터"""
    try:
        with open("data/apartment_news.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


@st.cache_data(ttl=1800)
def load_region_news():
    """지역별 뉴스 데이터"""
    try:
        with open("data/news_headlines.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


@st.cache_data(ttl=1800)
def load_search_trend():
    """검색 트렌드 데이터"""
    try:
        with open("data/search_trend.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


# --- UI ---
tab1, tab2, tab3 = st.tabs(["🏢 아파트별 뉴스", "🏘️ 지역별 뉴스", "📊 검색 트렌드"])

with tab1:
    st.subheader("🏢 아파트별 맞춤 뉴스")

    apt_news = load_apartment_news()

    if apt_news is not None:
        available_apts = list(apt_news["apartments"].keys())

        col1, col2 = st.columns([2, 1])

        with col1:
            selected_apt = st.selectbox(
                "🏢 아파트 선택", available_apts, key="apt_news_select"
            )

        with col2:
            apt_data = apt_news["apartments"][selected_apt]
            relevance_emoji = (
                "🔥" if apt_data["relevance_score"] == "very_high" else "✅"
            )
            st.metric(f"{relevance_emoji} 관련 뉴스", f"{apt_data['news_count']}건")

        st.info(
            f"""
        **📍 {selected_apt}** ({apt_data['region']})
        {apt_data['summary']}
        """
        )

        st.markdown("#### 📋 관련 뉴스")

        for idx, news in enumerate(apt_data["items"], 1):
            with st.container():
                st.markdown(
                    f"""
                **{idx}. [{news['title']}]({news['link']})**
                <small style="color: #666;">📅 {news['pubDate']} | 📰 {news['source']} | 🎯 {news['relevance']}</small>

                > {news['description']}
                """,
                    unsafe_allow_html=True,
                )
                st.divider()

        # 전체 아파트 현황
        with st.expander("📊 전체 아파트별 뉴스 현황"):
            apt_overview = []
            for apt_name, data in apt_news["apartments"].items():
                apt_overview.append(
                    {
                        "아파트": apt_name,
                        "지역": data["region"],
                        "관련 뉴스": data["news_count"],
                        "관련도": (
                            "🔥 매우높음"
                            if data["relevance_score"] == "very_high"
                            else "✅ 높음"
                        ),
                        "요약": data["summary"][:50] + "...",
                    }
                )

            apt_df = pd.DataFrame(apt_overview)
            apt_df = apt_df.sort_values("관련 뉴스", ascending=False)
            st.dataframe(apt_df, use_container_width=True, hide_index=True)
    else:
        st.warning("⚠️ 아파트 뉴스 데이터가 없습니다.")
        st.info("💡 데이터 수집 스크립트를 실행하여 뉴스를 수집해주세요.")

with tab2:
    st.subheader("🏘️ 지역별 부동산 뉴스")

    region_news = load_region_news()

    if region_news is not None:
        available_regions = list(region_news["regions"].keys())

        col1, col2 = st.columns([1, 3])

        with col1:
            selected_region = st.selectbox(
                "🏘️ 지역 선택", available_regions, key="region_news_select"
            )

            st.caption(f"🕐 업데이트: {region_news['last_updated'][:10]}")

            region_data = region_news["regions"][selected_region]
            st.metric("총 관련 뉴스", f"{region_data['total_news']:,}건")

        with col2:
            st.info(f"**📌 {selected_region} 요약**  \n{region_data['summary']}")

            st.markdown("#### 📋 최신 뉴스")

            for idx, news in enumerate(region_data["items"], 1):
                with st.container():
                    st.markdown(
                        f"""
                    **{idx}. [{news['title']}]({news['link']})**
                    <small style="color: gray;">📅 {news['pubDate']} | 📰 {news['source']}</small>

                    > {news['description'][:200]}{'...' if len(news['description']) > 200 else ''}
                    """,
                        unsafe_allow_html=True,
                    )
                    st.divider()
    else:
        st.warning("⚠️ 지역별 뉴스 데이터가 없습니다.")
        st.info("💡 데이터 수집 스크립트를 실행하여 뉴스를 수집해주세요.")

with tab3:
    st.subheader("📊 네이버 검색 트렌드")

    trend_data = load_search_trend()

    if trend_data is not None:
        import plotly.express as px

        # JSON을 DataFrame으로 변환
        all_dfs = []
        for group in trend_data:
            temp_df = pd.DataFrame(group["data"])
            temp_df["keyword"] = group["title"]
            all_dfs.append(temp_df)

        trend_df = pd.concat(all_dfs)
        trend_df["period"] = pd.to_datetime(trend_df["period"])

        # 키워드 선택
        keywords = trend_df["keyword"].unique().tolist()
        selected_keywords = st.multiselect("키워드 선택", keywords, default=keywords)

        if selected_keywords:
            filtered_df = trend_df[trend_df["keyword"].isin(selected_keywords)]

            fig = px.line(
                filtered_df,
                x="period",
                y="ratio",
                color="keyword",
                title="네이버 검색 트렌드",
                labels={
                    "period": "날짜",
                    "ratio": "검색량 (상대값)",
                    "keyword": "키워드",
                },
                markers=True,
            )
            st.plotly_chart(fig, use_container_width=True)

            # 인사이트
            st.markdown("#### 💡 트렌드 인사이트")

            for keyword in selected_keywords:
                kw_df = filtered_df[filtered_df["keyword"] == keyword].sort_values(
                    "period"
                )
                if len(kw_df) >= 2:
                    latest = kw_df.iloc[-1]["ratio"]
                    prev = kw_df.iloc[-2]["ratio"]
                    diff = latest - prev

                    if diff > 5:
                        st.success(f"🔥 **{keyword}**: 관심도 상승 (+{diff:.1f}p)")
                    elif diff < -5:
                        st.warning(f"❄️ **{keyword}**: 관심도 하락 ({diff:.1f}p)")
                    else:
                        st.info(f"➖ **{keyword}**: 관심도 유지 ({diff:+.1f}p)")
        else:
            st.info("키워드를 선택해주세요.")
    else:
        st.warning("⚠️ 검색 트렌드 데이터가 없습니다.")
        st.info("💡 MCP를 통해 네이버 검색 트렌드를 수집해주세요.")
