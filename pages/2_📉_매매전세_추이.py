from datetime import date

import altair as alt
import pandas as pd
import streamlit as st

from utils.bq_client import (
    FILTER_EXCLUDE_JUSANGBOKHAP,
    TABLE_COMPLEX,
    TABLE_JEONSAE,
    TABLE_MAEMAE,
    get_bq_client,
)

st.set_page_config(page_title="매매/전세 추이", page_icon="📉", layout="wide")

st.title("📉 매매/전세 추이")
st.markdown("동별, 아파트별 **매매가**와 **전세가**의 시계열 변화를 분석합니다.")

# --- 사이드바: 기간 선택 ---
with st.sidebar:
    st.header("📅 데이터 기간 설정")

    # 기본값: 2025-01-01 ~ 오늘
    default_start = date(2025, 1, 1)
    default_end = date.today()

    date_range = st.date_input(
        "조회 기간",
        value=(default_start, default_end),
        min_value=date(2020, 1, 1),
        max_value=date.today(),
        key="date_range",
    )

    # 날짜 범위 파싱
    if len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = default_start, default_end

    st.caption(f"📊 {start_date.strftime('%Y.%m')} ~ {end_date.strftime('%Y.%m')}")

st.markdown("---")

# 색상 팔레트 (아파트 비교용)
APARTMENT_COLORS = [
    "#FF6B6B",
    "#4ECDC4",
    "#45B7D1",
    "#96CEB4",
    "#FFEAA7",
    "#DDA0DD",
    "#98D8C8",
    "#F7DC6F",
]


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
def load_apartments_price_history(apartment_names: tuple):
    """여러 아파트의 매매/전세 월간 평균 이력"""
    client = get_bq_client()

    # apartment_names를 SQL IN 절에 사용할 수 있도록 변환
    apt_list_str = ", ".join([f"'{apt}'" for apt in apartment_names])

    query = f"""
    WITH maemae AS (
        SELECT
            apartment_name,
            SUBSTR(date, 1, 7) as month,
            AVG(price) as price,
            COUNT(*) as trade_count,
            '매매' as type
        FROM `{TABLE_MAEMAE}`
        WHERE apartment_name IN ({apt_list_str})
          AND price IS NOT NULL
        GROUP BY apartment_name, month
    ),
    jeonsae AS (
        SELECT
            apartment_name,
            SUBSTR(date, 1, 7) as month,
            AVG(price) as price,
            COUNT(*) as trade_count,
            '전세' as type
        FROM `{TABLE_JEONSAE}`
        WHERE apartment_name IN ({apt_list_str})
          AND price IS NOT NULL
        GROUP BY apartment_name, month
    )
    SELECT * FROM maemae
    UNION ALL
    SELECT * FROM jeonsae
    ORDER BY month
    """
    df = client.query(query).to_dataframe()
    df["month"] = pd.to_datetime(df["month"] + "-01")
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
def create_comparison_chart(df: pd.DataFrame, trade_type: str, group_col: str, title: str):
    """비교 차트 생성 (지역별 또는 아파트별) + 3개월 이동평균선"""

    filtered = df[df["type"] == trade_type].copy()

    # 3개월 이동평균 계산 (그룹별로)
    filtered = filtered.sort_values(["month", group_col])
    filtered["ma3"] = filtered.groupby(group_col)["price_억"].transform(
        lambda x: x.rolling(window=3, min_periods=1).mean()
    )

    # 원본 데이터 라인 (점 포함, 연한 색상)
    base_line = (
        alt.Chart(filtered)
        .mark_line(point=True, strokeWidth=1.5, opacity=0.5)
        .encode(
            x=alt.X("month:T", title="월", axis=alt.Axis(format="%Y-%m", labelAngle=-45)),
            y=alt.Y("price_억:Q", title="평균가격 (억원)", scale=alt.Scale(zero=False)),
            color=alt.Color(f"{group_col}:N", legend=alt.Legend(title="", orient="top")),
            tooltip=[
                alt.Tooltip("month:T", title="월", format="%Y-%m"),
                alt.Tooltip(f"{group_col}:N", title="이름"),
                alt.Tooltip("price_억:Q", title="실제가(억)", format=".2f"),
                alt.Tooltip("ma3:Q", title="3개월MA(억)", format=".2f"),
                alt.Tooltip("trade_count:Q", title="거래건수"),
            ],
        )
    )

    # 3개월 이동평균선 (부드러운 곡선, 진한 색상)
    ma_line = (
        alt.Chart(filtered)
        .mark_line(strokeWidth=3, interpolate="monotone")  # 부드러운 곡선
        .encode(
            x="month:T",
            y=alt.Y("ma3:Q", title="평균가격 (억원)"),
            color=alt.Color(f"{group_col}:N"),
            strokeDash=alt.StrokeDash(f"{group_col}:N"),
        )
    )

    # 레이어 결합
    chart = (
        (base_line + ma_line)
        .properties(
            title=alt.TitleParams(text=title, subtitle="실선: 3개월 이동평균 | 점선: 실제 거래가", fontSize=16),
            height=400,
        )
        .interactive()
    )

    return chart


def create_trade_volume_chart(df: pd.DataFrame, group_col: str, trade_type: str = None):
    """거래량 차트 (dodge 적용)"""

    trade_df = df.groupby(["month", group_col, "type"])["trade_count"].sum().reset_index()

    # 특정 거래유형만 필터링
    if trade_type:
        trade_df = trade_df[trade_df["type"] == trade_type]

    chart = (
        alt.Chart(trade_df)
        .mark_bar(opacity=0.8)
        .encode(
            x=alt.X("month:T", title="월", axis=alt.Axis(format="%Y-%m", labelAngle=-45)),
            y=alt.Y("trade_count:Q", title="거래건수"),
            color=alt.Color(f"{group_col}:N", legend=alt.Legend(title="", orient="top")),
            xOffset=alt.XOffset(f"{group_col}:N"),  # dodge 효과
            tooltip=[
                alt.Tooltip("month:T", format="%Y-%m", title="월"),
                alt.Tooltip(f"{group_col}:N", title="이름"),
                alt.Tooltip("type:N", title="거래유형"),
                alt.Tooltip("trade_count:Q", title="건수"),
            ],
        )
        .properties(
            title=f"📊 월별 거래량 ({trade_type})" if trade_type else "📊 월별 거래량",
            height=350,
        )
        .interactive()
    )

    return chart


def create_jeonse_rate_chart(df: pd.DataFrame, group_col: str):
    """전세가율 추이 차트"""

    # 매매/전세 데이터를 피벗하여 전세가율 계산
    pivot_df = df.pivot_table(
        index=["month", group_col], columns="type", values="price_억", aggfunc="mean"
    ).reset_index()

    # 전세가율 계산 (매매가, 전세가 모두 있는 경우만)
    if "매매" in pivot_df.columns and "전세" in pivot_df.columns:
        pivot_df["전세가율"] = (pivot_df["전세"] / pivot_df["매매"]) * 100
        pivot_df = pivot_df.dropna(subset=["전세가율"])
    else:
        return None

    if pivot_df.empty:
        return None

    chart = (
        alt.Chart(pivot_df)
        .mark_line(point=True, strokeWidth=2.5)
        .encode(
            x=alt.X("month:T", title="월", axis=alt.Axis(format="%Y-%m", labelAngle=-45)),
            y=alt.Y("전세가율:Q", title="전세가율 (%)", scale=alt.Scale(zero=False)),
            color=alt.Color(f"{group_col}:N", legend=alt.Legend(title="", orient="top")),
            strokeDash=alt.StrokeDash(f"{group_col}:N"),
            tooltip=[
                alt.Tooltip("month:T", title="월", format="%Y-%m"),
                alt.Tooltip(f"{group_col}:N", title="이름"),
                alt.Tooltip("전세가율:Q", title="전세가율(%)", format=".1f"),
                alt.Tooltip("매매:Q", title="매매가(억)", format=".2f"),
                alt.Tooltip("전세:Q", title="전세가(억)", format=".2f"),
            ],
        )
        .properties(
            title="📈 전세가율 추이",
            height=350,
        )
        .interactive()
    )

    return chart


# --- UI ---
# 탭 순서 변경: 동(지역)별 추이가 먼저
tab1, tab2 = st.tabs(["🏘️ 동(지역)별 추이", "🏢 아파트별 추이"])

# ==================== 동(지역)별 추이 ====================
with tab1:
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
                key="region_select",
            )

            if selected_regions:
                # 지역 필터링
                filtered_df = region_df[region_df["region"].isin(selected_regions)]

                # 기간 필터링
                filtered_df = filtered_df[
                    (filtered_df["month"] >= pd.Timestamp(start_date))
                    & (filtered_df["month"] <= pd.Timestamp(end_date))
                ]

                st.markdown("---")

                # 매매/전세 분리 차트
                col1, col2 = st.columns(2)

                with col1:
                    maemae_chart = create_comparison_chart(filtered_df, "매매", "region", "📈 매매가 추이")
                    st.altair_chart(maemae_chart, use_container_width=True)

                with col2:
                    jeonsae_chart = create_comparison_chart(filtered_df, "전세", "region", "📉 전세가 추이")
                    st.altair_chart(jeonsae_chart, use_container_width=True)

                # 거래량 & 전세가율 차트 (2열)
                col3, col4 = st.columns(2)

                with col3:
                    # 매매 거래량 (dodge 적용)
                    trade_chart = create_trade_volume_chart(filtered_df, "region", "매매")
                    st.altair_chart(trade_chart, use_container_width=True)

                with col4:
                    # 전세가율 추이
                    jeonse_rate_chart = create_jeonse_rate_chart(filtered_df, "region")
                    if jeonse_rate_chart:
                        st.altair_chart(jeonse_rate_chart, use_container_width=True)
                    else:
                        st.info("전세가율 데이터가 부족합니다.")

            else:
                st.info("비교할 지역을 선택해주세요.")
        else:
            st.warning("데이터가 없습니다.")

    except Exception as e:
        st.error(f"데이터 로딩 오류: {e}")


# ==================== 아파트별 추이 ====================
with tab2:
    st.subheader("🏢 아파트별 매매/전세 추이")

    try:
        apt_list = load_available_apartments()

        if not apt_list.empty:
            # 지역 복수 선택
            regions = sorted(apt_list["region"].unique().tolist())
            selected_regions = st.multiselect(
                "🏘️ 지역(동) 선택 (복수 선택 가능)",
                regions,
                default=regions[:1] if regions else [],
                key="apt_regions",
            )

            # 선택한 지역들의 아파트 목록
            if selected_regions:
                # 지역별로 아파트 이름에 지역 표시 추가 (동명이 다른 경우 구분)
                apts_in_regions = (
                    apt_list[apt_list["region"].isin(selected_regions)]
                    .apply(lambda x: f"{x['apartment_name']} ({x['region']})", axis=1)
                    .tolist()
                )
                # 원본 아파트 이름 매핑
                apt_display_to_name = dict(
                    zip(
                        apts_in_regions,
                        apt_list[apt_list["region"].isin(selected_regions)]["apartment_name"].tolist(),
                    )
                )
            else:
                apts_in_regions = []
                apt_display_to_name = {}

            # 아파트 복수 선택
            selected_apt_displays = st.multiselect(
                "🏢 비교할 아파트 선택 (최대 5개)",
                apts_in_regions,
                default=[],
                max_selections=5,
                key="apt_multi_select",
            )

            # 실제 아파트 이름으로 변환
            selected_apts = [apt_display_to_name[d] for d in selected_apt_displays if d in apt_display_to_name]

            if selected_apts:
                st.markdown("---")

                with st.spinner("데이터 로딩 중..."):
                    # 여러 아파트 데이터 한 번에 로딩
                    price_df = load_apartments_price_history(tuple(selected_apts))

                    # 기간 필터링
                    price_df = price_df[
                        (price_df["month"] >= pd.Timestamp(start_date)) & (price_df["month"] <= pd.Timestamp(end_date))
                    ]

                if not price_df.empty:
                    # 매매/전세 분리 차트 (동별과 동일한 레이아웃)
                    col1, col2 = st.columns(2)

                    with col1:
                        maemae_chart = create_comparison_chart(price_df, "매매", "apartment_name", "📈 매매가 추이")
                        st.altair_chart(maemae_chart, use_container_width=True)

                    with col2:
                        jeonsae_chart = create_comparison_chart(price_df, "전세", "apartment_name", "📉 전세가 추이")
                        st.altair_chart(jeonsae_chart, use_container_width=True)

                    # 거래량 & 전세가율 차트 (2열)
                    col3, col4 = st.columns(2)

                    with col3:
                        # 매매 거래량 (dodge 적용)
                        trade_chart = create_trade_volume_chart(price_df, "apartment_name", "매매")
                        st.altair_chart(trade_chart, use_container_width=True)

                    with col4:
                        # 전세가율 추이
                        jeonse_rate_chart = create_jeonse_rate_chart(price_df, "apartment_name")
                        if jeonse_rate_chart:
                            st.altair_chart(jeonse_rate_chart, use_container_width=True)
                        else:
                            st.info("전세가율 데이터가 부족합니다.")

                    # 최근 시세 요약 테이블
                    st.markdown("#### 📋 최근 시세 요약")

                    summary_data = []
                    for apt in selected_apts:
                        apt_df = price_df[price_df["apartment_name"] == apt]

                        maemae_df = apt_df[apt_df["type"] == "매매"].sort_values("month", ascending=False)
                        jeonsae_df = apt_df[apt_df["type"] == "전세"].sort_values("month", ascending=False)

                        latest_maemae = maemae_df.iloc[0]["price_억"] if not maemae_df.empty else None
                        latest_jeonsae = jeonsae_df.iloc[0]["price_억"] if not jeonsae_df.empty else None

                        jeonse_rate = None
                        if latest_maemae and latest_jeonsae:
                            jeonse_rate = (latest_jeonsae / latest_maemae) * 100

                        summary_data.append(
                            {
                                "아파트": apt,
                                "최근 매매가": f"{latest_maemae:.2f}억" if latest_maemae else "-",
                                "최근 전세가": f"{latest_jeonsae:.2f}억" if latest_jeonsae else "-",
                                "전세가율": f"{jeonse_rate:.1f}%" if jeonse_rate else "-",
                                "갭": f"{latest_maemae - latest_jeonsae:.2f}억"
                                if latest_maemae and latest_jeonsae
                                else "-",
                            }
                        )

                    summary_df = pd.DataFrame(summary_data)
                    st.dataframe(summary_df, use_container_width=True, hide_index=True)

                else:
                    st.warning("선택한 아파트의 거래 데이터가 없습니다.")
            else:
                st.info("비교할 아파트를 선택해주세요.")
        else:
            st.warning("아파트 목록을 불러올 수 없습니다.")

    except Exception as e:
        st.error(f"데이터 로딩 오류: {e}")
