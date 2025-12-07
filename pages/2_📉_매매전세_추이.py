import sys

import altair as alt
import pandas as pd
import streamlit as st

sys.path.append(".")

from utils.bq_client import (
    FILTER_EXCLUDE_JUSANGBOKHAP,
    TABLE_COMPLEX,
    TABLE_JEONSAE,
    TABLE_MAEMAE,
    get_bq_client,
)

st.set_page_config(page_title="매매/전세 추이", page_icon="📉", layout="wide")

st.title("📉 매매/전세 추이")
st.markdown("아파트별, 동별 **매매가**와 **전세가**의 시계열 변화를 분석합니다.")
st.markdown("---")

# 색상 팔레트
COLORS = {
    "매매": "#FF6B6B",  # 따뜻한 코랄
    "전세": "#4ECDC4",  # 청록색
}


# --- 데이터 로딩 ---
@st.cache_data(ttl=3600)
def load_available_apartments():
    """분석 가능한 아파트 목록 (주상복합 제외)"""
    client = get_bq_client()
    query = f"""
    SELECT DISTINCT apartment_name, region
    FROM `{TABLE_COMPLEX}`
    WHERE {FILTER_EXCLUDE_JUSANGBOKHAP}
    ORDER BY region, apartment_name
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_apartment_price_history(apartment_name: str):
    """특정 아파트의 매매/전세 이력"""
    client = get_bq_client()
    query = f"""
    WITH maemae AS (
        SELECT
            date,
            area_type,
            AVG(price) as price,
            '매매' as type
        FROM `{TABLE_MAEMAE}`
        WHERE apartment_name = '{apartment_name}'
          AND price IS NOT NULL
        GROUP BY date, area_type
    ),
    jeonsae AS (
        SELECT
            date,
            area_type,
            AVG(price) as price,
            '전세' as type
        FROM `{TABLE_JEONSAE}`
        WHERE apartment_name = '{apartment_name}'
          AND price IS NOT NULL
        GROUP BY date, area_type
    )
    SELECT * FROM maemae
    UNION ALL
    SELECT * FROM jeonsae
    ORDER BY date
    """
    df = client.query(query).to_dataframe()
    df["date"] = pd.to_datetime(df["date"])
    df["price_억"] = df["price"] / 10000
    return df


@st.cache_data(ttl=3600)
def load_region_price_trend():
    """동별 월간 평균가 추이 (주상복합 제외)"""
    client = get_bq_client()
    query = f"""
    WITH maemae_monthly AS (
        SELECT
            region,
            SUBSTR(date, 1, 7) as month,
            AVG(price) as avg_price,
            COUNT(*) as trade_count,
            '매매' as type
        FROM `{TABLE_MAEMAE}`
        WHERE price IS NOT NULL
          AND date >= '2023-01-01'
          AND {FILTER_EXCLUDE_JUSANGBOKHAP}
        GROUP BY region, month
    ),
    jeonsae_monthly AS (
        SELECT
            region,
            SUBSTR(date, 1, 7) as month,
            AVG(price) as avg_price,
            COUNT(*) as trade_count,
            '전세' as type
        FROM `{TABLE_JEONSAE}`
        WHERE price IS NOT NULL
          AND date >= '2023-01-01'
          AND {FILTER_EXCLUDE_JUSANGBOKHAP}
        GROUP BY region, month
    )
    SELECT * FROM maemae_monthly
    UNION ALL
    SELECT * FROM jeonsae_monthly
    ORDER BY month
    """
    df = client.query(query).to_dataframe()
    df["month"] = pd.to_datetime(df["month"] + "-01")
    df["price_억"] = df["avg_price"] / 10000
    return df


# --- 차트 함수 ---
def create_price_chart(df: pd.DataFrame, title: str, area_type: str = None):
    """매매/전세 추이 Altair 차트 생성"""

    chart_df = df.copy()

    # 기본 선 차트
    base = alt.Chart(chart_df).encode(
        x=alt.X("date:T", title="날짜", axis=alt.Axis(format="%Y-%m", labelAngle=-45)),
        y=alt.Y("price_억:Q", title="가격 (억원)", scale=alt.Scale(zero=False)),
        color=alt.Color(
            "type:N",
            scale=alt.Scale(
                domain=["매매", "전세"], range=[COLORS["매매"], COLORS["전세"]]
            ),
            legend=alt.Legend(title="거래유형", orient="top"),
        ),
        tooltip=[
            alt.Tooltip("date:T", title="날짜", format="%Y-%m-%d"),
            alt.Tooltip("type:N", title="유형"),
            alt.Tooltip("price_억:Q", title="가격(억)", format=".2f"),
            alt.Tooltip("area_type:N", title="평형"),
        ],
    )

    # 선 + 점 레이어
    line = base.mark_line(strokeWidth=2.5, opacity=0.8)
    points = base.mark_circle(size=60, opacity=0.9)

    # 결합
    chart = (
        (line + points)
        .properties(
            title=alt.TitleParams(text=title, fontSize=16, anchor="start"), height=350
        )
        .configure_axis(labelFontSize=11, titleFontSize=12, gridOpacity=0.3)
        .configure_legend(labelFontSize=12, titleFontSize=12)
        .interactive()
    )

    return chart


def create_area_chart(df: pd.DataFrame, title: str):
    """매매/전세 영역 차트 (갭 시각화)"""

    # 피벗으로 매매/전세 분리
    pivot_df = df.pivot_table(
        index="date", columns="type", values="price_억", aggfunc="mean"
    ).reset_index()

    if "매매" not in pivot_df.columns or "전세" not in pivot_df.columns:
        return None

    pivot_df["gap"] = pivot_df["매매"] - pivot_df["전세"]

    # 기본 차트
    base = alt.Chart(pivot_df).encode(
        x=alt.X("date:T", title="날짜", axis=alt.Axis(format="%Y-%m"))
    )

    # 매매가 라인
    maemae_line = base.mark_line(color=COLORS["매매"], strokeWidth=3).encode(
        y=alt.Y("매매:Q", title="가격 (억원)", scale=alt.Scale(zero=False)),
        tooltip=[
            alt.Tooltip("date:T", format="%Y-%m-%d"),
            alt.Tooltip("매매:Q", format=".2f", title="매매가"),
        ],
    )

    # 전세가 라인
    jeonsae_line = base.mark_line(color=COLORS["전세"], strokeWidth=3).encode(
        y="전세:Q",
        tooltip=[
            alt.Tooltip("date:T", format="%Y-%m-%d"),
            alt.Tooltip("전세:Q", format=".2f", title="전세가"),
        ],
    )

    # 갭 영역 (매매-전세 사이)
    area = base.mark_area(opacity=0.15, color="#FFD93D").encode(y="전세:Q", y2="매매:Q")

    chart = (
        (area + jeonsae_line + maemae_line)
        .properties(
            title=alt.TitleParams(
                text=title, subtitle="음영: 매매-전세 갭", fontSize=16
            ),
            height=400,
        )
        .interactive()
    )

    return chart


def create_region_comparison_chart(df: pd.DataFrame, trade_type: str):
    """지역별 비교 차트"""

    filtered = df[df["type"] == trade_type]

    chart = (
        alt.Chart(filtered)
        .mark_line(point=True, strokeWidth=2.5)
        .encode(
            x=alt.X(
                "month:T", title="월", axis=alt.Axis(format="%Y-%m", labelAngle=-45)
            ),
            y=alt.Y("price_억:Q", title="평균가격 (억원)", scale=alt.Scale(zero=False)),
            color=alt.Color(
                "region:N", legend=alt.Legend(title="지역", orient="right")
            ),
            strokeDash=alt.StrokeDash("region:N"),
            tooltip=[
                alt.Tooltip("month:T", title="월", format="%Y-%m"),
                alt.Tooltip("region:N", title="지역"),
                alt.Tooltip("price_억:Q", title="평균가(억)", format=".2f"),
                alt.Tooltip("trade_count:Q", title="거래건수"),
            ],
        )
        .properties(
            title=f"{'📈 매매가' if trade_type == '매매' else '📉 전세가'} 추이",
            height=350,
        )
        .interactive()
    )

    return chart


# --- UI ---
tab1, tab2 = st.tabs(["🏢 아파트별 추이", "🏘️ 동(지역)별 추이"])

with tab1:
    st.subheader("🏢 아파트별 매매/전세 추이")

    try:
        apt_list = load_available_apartments()

        if not apt_list.empty:
            # 지역 -> 아파트 연계 선택
            col1, col2 = st.columns(2)

            with col1:
                regions = sorted(apt_list["region"].unique().tolist())
                selected_region = st.selectbox(
                    "🏘️ 지역(동) 선택", regions, key="apt_region"
                )

            with col2:
                apts_in_region = apt_list[apt_list["region"] == selected_region][
                    "apartment_name"
                ].tolist()
                selected_apt = st.selectbox(
                    "🏢 아파트 선택", apts_in_region, key="apt_name"
                )

            if selected_apt:
                st.markdown("---")

                with st.spinner(f"'{selected_apt}' 데이터 로딩 중..."):
                    price_df = load_apartment_price_history(selected_apt)

                if not price_df.empty:
                    # 평형 선택
                    area_types = ["전체"] + sorted(
                        price_df["area_type"].unique().tolist()
                    )
                    selected_area = st.selectbox("📐 평형 선택", area_types)

                    if selected_area != "전체":
                        chart_df = price_df[price_df["area_type"] == selected_area]
                    else:
                        chart_df = price_df

                    # 최근 거래 요약 (상단 KPI)
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        recent_maemae = chart_df[
                            chart_df["type"] == "매매"
                        ].sort_values("date", ascending=False)
                        if not recent_maemae.empty:
                            latest = recent_maemae.iloc[0]
                            st.metric(
                                "🏷️ 최근 매매가",
                                f"{latest['price_억']:.2f}억",
                                f"{latest['date'].strftime('%Y-%m-%d')}",
                            )

                    with col2:
                        recent_jeonsae = chart_df[
                            chart_df["type"] == "전세"
                        ].sort_values("date", ascending=False)
                        if not recent_jeonsae.empty:
                            latest = recent_jeonsae.iloc[0]
                            st.metric(
                                "🔑 최근 전세가",
                                f"{latest['price_억']:.2f}억",
                                f"{latest['date'].strftime('%Y-%m-%d')}",
                            )

                    with col3:
                        if not recent_maemae.empty and not recent_jeonsae.empty:
                            gap = (
                                recent_maemae.iloc[0]["price_억"]
                                - recent_jeonsae.iloc[0]["price_억"]
                            )
                            rate = (
                                recent_jeonsae.iloc[0]["price_억"]
                                / recent_maemae.iloc[0]["price_억"]
                            ) * 100
                            st.metric("📊 전세가율", f"{rate:.1f}%", f"갭 {gap:.2f}억")

                    st.markdown("---")

                    # 메인 차트: 갭 영역 차트
                    st.markdown(f"#### 📈 {selected_apt} 시세 추이")

                    area_chart = create_area_chart(
                        chart_df, f"{selected_apt} 매매/전세 추이"
                    )
                    if area_chart:
                        st.altair_chart(area_chart, use_container_width=True)

                    # 평형별 상세 (전체 선택 시)
                    if (
                        selected_area == "전체"
                        and len(price_df["area_type"].unique()) > 1
                    ):
                        with st.expander("📐 평형별 상세 차트"):
                            for area in sorted(price_df["area_type"].unique()):
                                area_df = price_df[price_df["area_type"] == area]
                                chart = create_price_chart(area_df, f"{area} 타입")
                                st.altair_chart(chart, use_container_width=True)
                                st.divider()

                    # 상세 데이터
                    with st.expander("📋 상세 거래 내역"):
                        display_df = chart_df.copy()
                        display_df["가격"] = display_df["price_억"].apply(
                            lambda x: f"{x:.2f}억"
                        )
                        display_df["날짜"] = display_df["date"].dt.strftime("%Y-%m-%d")
                        display_df = display_df[["날짜", "area_type", "가격", "type"]]
                        display_df.columns = ["날짜", "평형", "가격", "거래유형"]
                        st.dataframe(
                            display_df.sort_values("날짜", ascending=False),
                            use_container_width=True,
                            hide_index=True,
                        )
                else:
                    st.warning(f"'{selected_apt}'의 거래 데이터가 없습니다.")
        else:
            st.warning("아파트 목록을 불러올 수 없습니다.")

    except Exception as e:
        st.error(f"데이터 로딩 오류: {e}")

with tab2:
    st.subheader("🏘️ 동별 월간 평균가 추이")

    try:
        region_df = load_region_price_trend()

        if not region_df.empty:
            # 지역 선택 (복수)
            regions = sorted(region_df["region"].unique().tolist())
            selected_regions = st.multiselect(
                "🏘️ 비교할 지역(동) 선택 (최대 5개)",
                regions,
                default=regions[:3] if len(regions) >= 3 else regions,
                max_selections=5,
            )

            if selected_regions:
                filtered_df = region_df[region_df["region"].isin(selected_regions)]

                st.markdown("---")

                # 매매/전세 분리 차트
                col1, col2 = st.columns(2)

                with col1:
                    maemae_chart = create_region_comparison_chart(filtered_df, "매매")
                    st.altair_chart(maemae_chart, use_container_width=True)

                with col2:
                    jeonsae_chart = create_region_comparison_chart(filtered_df, "전세")
                    st.altair_chart(jeonsae_chart, use_container_width=True)

                # 거래량 바 차트
                st.markdown("#### 📊 월별 거래량")

                trade_df = (
                    filtered_df.groupby(["month", "region", "type"])["trade_count"]
                    .sum()
                    .reset_index()
                )

                trade_chart = (
                    alt.Chart(trade_df)
                    .mark_bar(opacity=0.8)
                    .encode(
                        x=alt.X("month:T", title="월", axis=alt.Axis(format="%Y-%m")),
                        y=alt.Y("trade_count:Q", title="거래건수", stack=None),
                        color=alt.Color("region:N", legend=alt.Legend(orient="right")),
                        column=alt.Column(
                            "type:N",
                            title="거래유형",
                            header=alt.Header(labelFontSize=14),
                        ),
                        tooltip=[
                            alt.Tooltip("month:T", format="%Y-%m"),
                            alt.Tooltip("region:N"),
                            alt.Tooltip("trade_count:Q", title="건수"),
                        ],
                    )
                    .properties(width=350, height=250)
                    .interactive()
                )

                st.altair_chart(trade_chart, use_container_width=True)

            else:
                st.info("비교할 지역을 선택해주세요.")
        else:
            st.warning("데이터가 없습니다.")

    except Exception as e:
        st.error(f"데이터 로딩 오류: {e}")
