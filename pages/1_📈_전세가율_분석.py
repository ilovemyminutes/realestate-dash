"""
📈 전세가율 분석 페이지
- 아파트별/동별 전세가율 현황
- 갭투자 유망 단지
- 깡통전세 위험 경고
"""

import altair as alt
import pandas as pd
import streamlit as st

from utils.bq_client import (
    FILTER_EXCLUDE_JUSANGBOKHAP,
    TABLE_JEONSAE,
    TABLE_MAEMAE,
    get_bq_client,
)

st.set_page_config(page_title="전세가율 분석", page_icon="📈", layout="wide")

st.title("📈 전세가율 분석")
st.markdown("아파트별/동별 전세가율을 분석하여 **갭투자 유망 단지**와 **깡통전세 위험군**을 파악합니다.")
st.markdown("---")


# --- 데이터 로딩 ---
@st.cache_data(ttl=3600)
def load_jeonse_rate_by_region():
    """동별 전세가율 데이터"""
    client = get_bq_client()
    query = f"""
    WITH maemae_avg AS (
        SELECT
            region,
            apartment_name,
            area_type,
            AVG(price) as avg_maemae
        FROM `{TABLE_MAEMAE}`
        WHERE price IS NOT NULL
          AND date >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AS STRING)
          AND {FILTER_EXCLUDE_JUSANGBOKHAP}
        GROUP BY region, apartment_name, area_type
    ),
    jeonsae_avg AS (
        SELECT
            region,
            apartment_name,
            area_type,
            AVG(price) as avg_jeonsae
        FROM `{TABLE_JEONSAE}`
        WHERE price IS NOT NULL
          AND date >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AS STRING)
          AND {FILTER_EXCLUDE_JUSANGBOKHAP}
        GROUP BY region, apartment_name, area_type
    )
    SELECT
        m.region,
        m.apartment_name,
        m.area_type,
        ROUND(m.avg_maemae) as avg_maemae,
        ROUND(j.avg_jeonsae) as avg_jeonsae,
        ROUND(m.avg_maemae - j.avg_jeonsae) as gap,
        ROUND(j.avg_jeonsae / m.avg_maemae * 100, 1) as jeonse_rate
    FROM maemae_avg m
    JOIN jeonsae_avg j
        ON m.region = j.region
        AND m.apartment_name = j.apartment_name
        AND m.area_type = j.area_type
    WHERE m.avg_maemae > 0
    ORDER BY jeonse_rate DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_jeonse_rate_summary_by_region():
    """동별 평균 전세가율 요약"""
    client = get_bq_client()
    query = f"""
    WITH maemae_avg AS (
        SELECT
            region,
            AVG(price) as avg_maemae
        FROM `{TABLE_MAEMAE}`
        WHERE price IS NOT NULL
          AND date >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AS STRING)
          AND {FILTER_EXCLUDE_JUSANGBOKHAP}
        GROUP BY region
    ),
    jeonsae_avg AS (
        SELECT
            region,
            AVG(price) as avg_jeonsae
        FROM `{TABLE_JEONSAE}`
        WHERE price IS NOT NULL
          AND date >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AS STRING)
          AND {FILTER_EXCLUDE_JUSANGBOKHAP}
        GROUP BY region
    )
    SELECT
        m.region,
        ROUND(m.avg_maemae) as avg_maemae,
        ROUND(j.avg_jeonsae) as avg_jeonsae,
        ROUND(m.avg_maemae - j.avg_jeonsae) as gap,
        ROUND(j.avg_jeonsae / m.avg_maemae * 100, 1) as jeonse_rate
    FROM maemae_avg m
    JOIN jeonsae_avg j ON m.region = j.region
    WHERE m.avg_maemae > 0
    ORDER BY jeonse_rate DESC
    """
    return client.query(query).to_dataframe()


# --- 차트 함수 ---
def create_jeonse_rate_bar_chart(df: pd.DataFrame):
    """동별 전세가율 바 차트 (Altair) - 단순화 버전"""

    # 데이터 정렬 (전세가율 내림차순 - 높은 게 위로)
    sorted_df = df.sort_values("jeonse_rate", ascending=True).copy()

    # 바 차트
    chart = (
        alt.Chart(sorted_df)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            x=alt.X("jeonse_rate:Q", title="전세가율 (%)", scale=alt.Scale(domain=[0, 100])),
            y=alt.Y("region:N", title="지역(동)", sort=list(sorted_df["region"])),
            color=alt.Color(
                "jeonse_rate:Q",
                scale=alt.Scale(scheme="redyellowgreen", reverse=True, domain=[20, 80]),
                legend=alt.Legend(title="전세가율(%)", orient="right"),
            ),
            tooltip=[
                alt.Tooltip("region:N", title="지역"),
                alt.Tooltip("jeonse_rate:Q", title="전세가율(%)", format=".1f"),
                alt.Tooltip("avg_maemae:Q", title="평균매매가(만원)", format=",.0f"),
                alt.Tooltip("avg_jeonsae:Q", title="평균전세가(만원)", format=",.0f"),
                alt.Tooltip("gap:Q", title="갭(만원)", format=",.0f"),
            ],
        )
        .properties(
            title=alt.TitleParams(
                text="동별 전세가율 현황 (6개월 평균)",
                subtitle="낮을수록 안전 (녹색) | 높을수록 위험 (빨간색)",
                fontSize=16,
                anchor="start",
            ),
            height=max(400, len(sorted_df) * 28),
        )
        .interactive()
    )

    return chart


def create_apartment_scatter_chart(df: pd.DataFrame):
    """아파트별 전세가율 산점도 차트"""

    df_copy = df.copy()
    df_copy["gap_억"] = df_copy["gap"] / 10000
    df_copy["avg_maemae_억"] = df_copy["avg_maemae"] / 10000

    chart = (
        alt.Chart(df_copy)
        .mark_circle(opacity=0.7)
        .encode(
            x=alt.X("avg_maemae_억:Q", title="평균 매매가 (억원)", scale=alt.Scale(zero=False)),
            y=alt.Y("jeonse_rate:Q", title="전세가율 (%)", scale=alt.Scale(domain=[30, 100])),
            size=alt.Size("gap_억:Q", title="갭(억)", scale=alt.Scale(range=[50, 500])),
            color=alt.Color(
                "jeonse_rate:Q",
                scale=alt.Scale(scheme="redyellowgreen", reverse=True, domain=[40, 90]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("apartment_name:N", title="아파트"),
                alt.Tooltip("region:N", title="지역"),
                alt.Tooltip("area_type:N", title="평형"),
                alt.Tooltip("jeonse_rate:Q", title="전세가율(%)", format=".1f"),
                alt.Tooltip("avg_maemae_억:Q", title="매매가(억)", format=".2f"),
                alt.Tooltip("gap_억:Q", title="갭(억)", format=".2f"),
            ],
        )
        .properties(
            title=alt.TitleParams(
                text="아파트별 전세가율 분포",
                subtitle="원 크기: 갭(억) | 색상: 전세가율",
                fontSize=16,
            ),
            height=400,
        )
        .interactive()
    )

    # 위험선 추가
    rule_70 = alt.Chart(pd.DataFrame({"y": [70]})).mark_rule(strokeDash=[5, 5], color="#FF6B6B").encode(y="y:Q")
    rule_80 = alt.Chart(pd.DataFrame({"y": [80]})).mark_rule(strokeDash=[5, 5], color="#DC143C").encode(y="y:Q")

    return chart + rule_70 + rule_80


# --- UI ---
tab1, tab2 = st.tabs(["🏘️ 동(지역)별 분석", "🏢 아파트별 분석"])

with tab1:
    st.subheader("🏘️ 동별 평균 전세가율")

    try:
        region_df = load_jeonse_rate_summary_by_region()

        if not region_df.empty:
            # KPI Cards
            col1, col2, col3, col4 = st.columns(4)

            highest = region_df.iloc[0]
            lowest = region_df.iloc[-1]
            avg_rate = region_df["jeonse_rate"].mean()
            danger_count = len(region_df[region_df["jeonse_rate"] >= 70])

            col1.metric("🔴 전세가율 최고", f"{highest['region']}", f"{highest['jeonse_rate']}%")
            col2.metric("🟢 전세가율 최저", f"{lowest['region']}", f"{lowest['jeonse_rate']}%")
            col3.metric("📊 전체 평균", f"{avg_rate:.1f}%")
            col4.metric("⚠️ 주의 지역", f"{danger_count}개", "70% 이상")

            st.markdown("---")

            # Altair 차트
            chart = create_jeonse_rate_bar_chart(region_df)
            st.altair_chart(chart, use_container_width=True)

            # 상세 테이블
            with st.expander("📋 상세 데이터 보기"):
                display_df = region_df.copy()
                display_df["avg_maemae"] = display_df["avg_maemae"].apply(lambda x: f"{x/10000:.1f}억")
                display_df["avg_jeonsae"] = display_df["avg_jeonsae"].apply(lambda x: f"{x/10000:.1f}억")
                display_df["gap"] = display_df["gap"].apply(lambda x: f"{x/10000:.1f}억")
                display_df.columns = [
                    "지역",
                    "평균매매가",
                    "평균전세가",
                    "갭(매매-전세)",
                    "전세가율(%)",
                ]
                st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.warning("데이터가 없습니다.")

    except Exception as e:
        st.error(f"데이터 로딩 오류: {e}")

with tab2:
    st.subheader("🏢 아파트별 전세가율")

    try:
        apt_df = load_jeonse_rate_by_region()

        if not apt_df.empty:
            # 필터
            col1, col2 = st.columns(2)

            with col1:
                regions = ["전체"] + sorted(apt_df["region"].unique().tolist())
                selected_region = st.selectbox("🏘️ 지역(동) 선택", regions)

            with col2:
                rate_filter = st.slider("📊 전세가율 범위 (%)", min_value=0, max_value=100, value=(40, 90))

            # 필터 적용
            filtered_df = apt_df.copy()
            if selected_region != "전체":
                filtered_df = filtered_df[filtered_df["region"] == selected_region]
            filtered_df = filtered_df[
                (filtered_df["jeonse_rate"] >= rate_filter[0]) & (filtered_df["jeonse_rate"] <= rate_filter[1])
            ]

            st.markdown("---")

            # 산점도 차트
            if not filtered_df.empty:
                scatter_chart = create_apartment_scatter_chart(filtered_df)
                st.altair_chart(scatter_chart, use_container_width=True)

            # 위험군 분류
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### 🔥 갭투자 유망 (전세가율 60~70%)")
                gap_invest = filtered_df[(filtered_df["jeonse_rate"] >= 60) & (filtered_df["jeonse_rate"] < 70)].head(
                    10
                )

                if not gap_invest.empty:
                    for _, row in gap_invest.iterrows():
                        gap_억 = row["gap"] / 10000
                        st.success(
                            f"**{row['apartment_name']}** ({row['area_type']})  \n"
                            f"📍 {row['region']} | 전세가율: **{row['jeonse_rate']}%** | 갭: **{gap_억:.1f}억**"
                        )
                else:
                    st.info("해당 조건의 단지가 없습니다.")

            with col2:
                st.markdown("#### ⚠️ 깡통전세 주의 (전세가율 80% 이상)")
                danger = filtered_df[filtered_df["jeonse_rate"] >= 80].head(10)

                if not danger.empty:
                    for _, row in danger.iterrows():
                        gap_억 = row["gap"] / 10000
                        st.error(
                            f"**{row['apartment_name']}** ({row['area_type']})  \n"
                            f"📍 {row['region']} | 전세가율: **{row['jeonse_rate']}%** | 갭: **{gap_억:.1f}억**"
                        )
                else:
                    st.success("깡통전세 위험 단지가 없습니다! 👍")

            # 전체 리스트
            with st.expander(f"📋 전체 목록 ({len(filtered_df)}건)"):
                display_df = filtered_df.copy()
                display_df["avg_maemae"] = display_df["avg_maemae"].apply(lambda x: f"{x/10000:.1f}억")
                display_df["avg_jeonsae"] = display_df["avg_jeonsae"].apply(lambda x: f"{x/10000:.1f}억")
                display_df["gap"] = display_df["gap"].apply(lambda x: f"{x/10000:.1f}억")
                display_df.columns = [
                    "지역",
                    "아파트",
                    "평형",
                    "평균매매가",
                    "평균전세가",
                    "갭",
                    "전세가율(%)",
                ]
                st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.warning("데이터가 없습니다.")

    except Exception as e:
        st.error(f"데이터 로딩 오류: {e}")
