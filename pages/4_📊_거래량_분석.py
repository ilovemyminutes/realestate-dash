"""
거래량 분석 페이지
- 동별 월간 매매/전세 거래량 추이
- 동별 총 세대수 (공급 규모)
- 신규 입주 예정 단지
- 거래량 vs 전세가율 관계 분석
"""

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.bq_client import (
    FILTER_EXCLUDE_JUSANGBOKHAP,
    TABLE_COMPLEX,
    TABLE_JEONSAE,
    TABLE_MAEMAE,
    get_bq_client,
)

st.set_page_config(page_title="거래량 분석", page_icon="📊", layout="wide")
st.title("📊 거래량 분석")
st.caption("동별 매매/전세 거래량 추이 및 공급 규모 분석")


# --- 데이터 로딩 ---
@st.cache_data(ttl=3600)
def load_monthly_trade_volume():
    """동별 월간 거래량"""
    client = get_bq_client()
    query = f"""
    WITH maemae_trades AS (
        SELECT
            region,
            DATE_TRUNC(PARSE_DATE('%Y-%m-%d', date), MONTH) as month,
            COUNT(*) as maemae_count
        FROM `{TABLE_MAEMAE}`
        WHERE price IS NOT NULL
          AND date >= '2024-01-01'
          AND {FILTER_EXCLUDE_JUSANGBOKHAP}
        GROUP BY region, month
    ),
    jeonsae_trades AS (
        SELECT
            region,
            DATE_TRUNC(PARSE_DATE('%Y-%m-%d', date), MONTH) as month,
            COUNT(*) as jeonsae_count
        FROM `{TABLE_JEONSAE}`
        WHERE price IS NOT NULL
          AND date >= '2024-01-01'
          AND {FILTER_EXCLUDE_JUSANGBOKHAP}
        GROUP BY region, month
    )
    SELECT
        COALESCE(m.region, j.region) as region,
        COALESCE(m.month, j.month) as month,
        COALESCE(m.maemae_count, 0) as maemae_count,
        COALESCE(j.jeonsae_count, 0) as jeonsae_count
    FROM maemae_trades m
    FULL OUTER JOIN jeonsae_trades j
        ON m.region = j.region AND m.month = j.month
    ORDER BY region, month
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_region_supply():
    """동별 총 세대수 및 아파트 단지 수"""
    client = get_bq_client()
    query = f"""
    SELECT
        region,
        COUNT(DISTINCT apartment_name) as apt_count,
        SUM(total_households) as total_households,
        AVG(building_age) as avg_building_age,
        COUNT(CASE WHEN building_age <= 10 THEN 1 END) as new_apt_count,
        COUNT(CASE WHEN building_age > 10 THEN 1 END) as old_apt_count
    FROM `{TABLE_COMPLEX}`
    WHERE {FILTER_EXCLUDE_JUSANGBOKHAP}
      AND total_households IS NOT NULL
    GROUP BY region
    ORDER BY total_households DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_upcoming_supply():
    """미입주/분양권 단지 (building_age < 0)"""
    client = get_bq_client()
    query = f"""
    SELECT
        region,
        apartment_name,
        construction_year,
        total_households,
        building_age
    FROM `{TABLE_COMPLEX}`
    WHERE building_age < 0
      AND {FILTER_EXCLUDE_JUSANGBOKHAP}
    ORDER BY construction_year DESC, total_households DESC
    """
    return client.query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_volume_vs_jeonse_rate():
    """동별 거래량과 전세가율 관계 데이터"""
    client = get_bq_client()
    query = f"""
    WITH maemae_trades AS (
        SELECT
            region,
            COUNT(*) as maemae_count,
            AVG(price) as avg_maemae
        FROM `{TABLE_MAEMAE}`
        WHERE price IS NOT NULL
          AND date >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AS STRING)
          AND {FILTER_EXCLUDE_JUSANGBOKHAP}
        GROUP BY region
    ),
    jeonsae_trades AS (
        SELECT
            region,
            COUNT(*) as jeonsae_count,
            AVG(price) as avg_jeonsae
        FROM `{TABLE_JEONSAE}`
        WHERE price IS NOT NULL
          AND date >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AS STRING)
          AND {FILTER_EXCLUDE_JUSANGBOKHAP}
        GROUP BY region
    ),
    complex_stats AS (
        SELECT
            region,
            SUM(total_households) as total_households,
            AVG(building_age) as avg_building_age
        FROM `{TABLE_COMPLEX}`
        WHERE {FILTER_EXCLUDE_JUSANGBOKHAP}
          AND total_households IS NOT NULL
        GROUP BY region
    )
    SELECT
        m.region,
        m.maemae_count,
        j.jeonsae_count,
        (m.maemae_count + j.jeonsae_count) as total_trades,
        ROUND(j.avg_jeonsae / NULLIF(m.avg_maemae, 0) * 100, 1) as jeonse_rate,
        ROUND(m.avg_maemae / 10000, 1) as avg_maemae_eok,
        ROUND(j.avg_jeonsae / 10000, 1) as avg_jeonsae_eok,
        c.total_households,
        ROUND(c.avg_building_age, 1) as avg_building_age
    FROM maemae_trades m
    JOIN jeonsae_trades j ON m.region = j.region
    LEFT JOIN complex_stats c ON m.region = c.region
    WHERE m.avg_maemae > 0
    ORDER BY total_trades DESC
    """
    return client.query(query).to_dataframe()


# --- 차트 함수 ---
def create_trade_volume_chart(df, selected_regions):
    """동별 거래량 추이 차트"""
    filtered = df[df["region"].isin(selected_regions)]

    # Long format으로 변환
    melted = filtered.melt(
        id_vars=["region", "month"],
        value_vars=["maemae_count", "jeonsae_count"],
        var_name="거래유형",
        value_name="거래량",
    )
    melted["거래유형"] = melted["거래유형"].map({"maemae_count": "매매", "jeonsae_count": "전세"})

    fig = px.line(
        melted,
        x="month",
        y="거래량",
        color="region",
        line_dash="거래유형",
        markers=True,
        title="동별 월간 거래량 추이",
        labels={"month": "월", "거래량": "거래 건수", "region": "지역"},
    )
    fig.update_layout(
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
    )
    return fig


def create_supply_bar_chart(df):
    """동별 세대수 막대 차트"""
    fig = px.bar(
        df.head(15),
        x="region",
        y="total_households",
        color="avg_building_age",
        color_continuous_scale="RdYlGn_r",
        title="동별 총 세대수 (상위 15개 동)",
        labels={"region": "지역(동)", "total_households": "총 세대수", "avg_building_age": "평균 연식"},
        text="total_households",
    )
    fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig.update_layout(height=400, xaxis_tickangle=-45)
    return fig


def create_new_old_ratio_chart(df):
    """동별 신축/구축 비율 차트"""
    df_sorted = df.sort_values("total_households", ascending=False).head(15)

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            name="신축 (10년 이하)",
            x=df_sorted["region"],
            y=df_sorted["new_apt_count"],
            marker_color="#4CAF50",
        )
    )
    fig.add_trace(
        go.Bar(
            name="구축 (10년 초과)",
            x=df_sorted["region"],
            y=df_sorted["old_apt_count"],
            marker_color="#FF7043",
        )
    )
    fig.update_layout(
        barmode="stack",
        title="동별 신축/구축 아파트 비율",
        xaxis_title="지역(동)",
        yaxis_title="단지 수",
        height=400,
        xaxis_tickangle=-45,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    return fig


# --- 메인 UI ---
tab1, tab2, tab3, tab4 = st.tabs(["📈 거래량 추이", "🔗 거래량 vs 전세가율", "🏠 공급 규모", "🆕 신규 입주 예정"])

# --- Tab 1: 거래량 추이 ---
with tab1:
    st.markdown("### 📈 동별 월간 거래량 추이")
    st.caption("2024년 1월 이후 실거래 데이터 기준")

    try:
        trade_df = load_monthly_trade_volume()

        if not trade_df.empty:
            # 지역 선택
            regions = sorted(trade_df["region"].unique().tolist())
            default_regions = regions[:3] if len(regions) >= 3 else regions

            selected_regions = st.multiselect(
                "🏘️ 비교할 지역 선택 (최대 5개)",
                regions,
                default=default_regions,
                max_selections=5,
            )

            if selected_regions:
                # 거래량 추이 차트
                fig = create_trade_volume_chart(trade_df, selected_regions)
                st.plotly_chart(fig, use_container_width=True)

                # 요약 테이블
                st.markdown("#### 📊 최근 3개월 거래량 요약")
                recent_df = trade_df[trade_df["region"].isin(selected_regions)]
                recent_df = recent_df.sort_values("month", ascending=False)

                # 최근 3개월만
                latest_months = recent_df["month"].unique()[:3]
                summary = recent_df[recent_df["month"].isin(latest_months)]

                pivot = summary.pivot_table(
                    index="region",
                    columns="month",
                    values=["maemae_count", "jeonsae_count"],
                    aggfunc="sum",
                ).fillna(0)

                st.dataframe(pivot.astype(int), use_container_width=True)

                # 매매/전세 비율 분석
                st.markdown("#### 📉 매매 vs 전세 거래 비율")
                col1, col2 = st.columns(2)

                for i, region in enumerate(selected_regions[:2]):
                    region_data = trade_df[trade_df["region"] == region]
                    total_maemae = region_data["maemae_count"].sum()
                    total_jeonsae = region_data["jeonsae_count"].sum()
                    total = total_maemae + total_jeonsae

                    with [col1, col2][i]:
                        st.metric(
                            f"📍 {region}",
                            f"매매 {total_maemae:,}건 / 전세 {total_jeonsae:,}건",
                            f"매매 비중: {total_maemae/total*100:.1f}%" if total > 0 else "데이터 없음",
                        )
            else:
                st.warning("비교할 지역을 선택해주세요.")
        else:
            st.warning("거래량 데이터가 없습니다.")

    except Exception as e:
        st.error(f"데이터 로딩 오류: {e}")

# --- Tab 2: 거래량 vs 전세가율 ---
with tab2:
    st.markdown("### 🔗 거래량과 전세가율의 관계")
    st.caption("최근 6개월 데이터 기준 | 원 크기: 총 세대수 | 색상: 평균 연식")

    try:
        vol_rate_df = load_volume_vs_jeonse_rate()

        if not vol_rate_df.empty:
            # 필터
            col1, col2 = st.columns(2)
            with col1:
                min_trades = st.slider("최소 거래량", 0, int(vol_rate_df["total_trades"].max()), 10)
            with col2:
                rate_range = st.slider("전세가율 범위 (%)", 0, 100, (30, 90))

            filtered = vol_rate_df[
                (vol_rate_df["total_trades"] >= min_trades)
                & (vol_rate_df["jeonse_rate"] >= rate_range[0])
                & (vol_rate_df["jeonse_rate"] <= rate_range[1])
            ]

            if not filtered.empty:
                # 산점도: 거래량 vs 전세가율
                fig_scatter = px.scatter(
                    filtered,
                    x="total_trades",
                    y="jeonse_rate",
                    size="total_households",
                    color="avg_building_age",
                    color_continuous_scale="RdYlGn_r",
                    hover_name="region",
                    hover_data={
                        "maemae_count": True,
                        "jeonsae_count": True,
                        "avg_maemae_eok": True,
                        "avg_jeonsae_eok": True,
                        "total_households": True,
                    },
                    labels={
                        "total_trades": "총 거래량 (건)",
                        "jeonse_rate": "전세가율 (%)",
                        "avg_building_age": "평균 연식",
                        "total_households": "총 세대수",
                    },
                    title="동별 거래량 vs 전세가율",
                )
                fig_scatter.update_layout(height=500)

                # 위험선 추가
                fig_scatter.add_hline(
                    y=70, line_dash="dash", line_color="#FFA726", line_width=1, annotation_text="⚠️ 70%"
                )
                fig_scatter.add_hline(
                    y=80, line_dash="dash", line_color="#EF5350", line_width=1, annotation_text="🚨 80%"
                )

                st.plotly_chart(fig_scatter, use_container_width=True)

                # 인사이트 분석
                st.markdown("---")
                st.markdown("#### 💡 인사이트")

                col1, col2 = st.columns(2)

                # 거래량 많고 전세가율 낮은 지역 (활발한 시장 + 안전)
                with col1:
                    st.markdown("##### ✅ 활발한 시장 + 안전 지역")
                    st.caption("거래량 상위 30% & 전세가율 60% 미만")
                    trade_threshold = filtered["total_trades"].quantile(0.7)
                    safe_active = filtered[
                        (filtered["total_trades"] >= trade_threshold) & (filtered["jeonse_rate"] < 60)
                    ]
                    if not safe_active.empty:
                        for _, row in safe_active.head(5).iterrows():
                            r, t, j = row["region"], row["total_trades"], row["jeonse_rate"]
                            st.success(f"**{r}** - 거래량: {t}건 | 전세가율: {j}%")
                    else:
                        st.info("해당 조건의 지역이 없습니다.")

                # 거래량 적고 전세가율 높은 지역 (침체 + 위험)
                with col2:
                    st.markdown("##### ⚠️ 침체 시장 + 위험 지역")
                    st.caption("거래량 하위 30% & 전세가율 70% 이상")
                    trade_low = filtered["total_trades"].quantile(0.3)
                    risky_stale = filtered[(filtered["total_trades"] <= trade_low) & (filtered["jeonse_rate"] >= 70)]
                    if not risky_stale.empty:
                        for _, row in risky_stale.head(5).iterrows():
                            r, t, j = row["region"], row["total_trades"], row["jeonse_rate"]
                            st.error(f"**{r}** - 거래량: {t}건 | 전세가율: {j}%")
                    else:
                        st.success("위험 지역이 없습니다! 👍")

                # 상관관계 분석
                st.markdown("---")
                st.markdown("#### 📊 상관관계 분석")

                correlation = filtered["total_trades"].corr(filtered["jeonse_rate"])
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric("거래량-전세가율 상관계수", f"{correlation:.3f}")
                with col2:
                    if correlation < -0.3:
                        st.info("📉 음의 상관: 거래 활발할수록 전세가율 낮음")
                    elif correlation > 0.3:
                        st.warning("📈 양의 상관: 거래 활발할수록 전세가율 높음")
                    else:
                        st.success("➡️ 약한 상관: 거래량과 전세가율은 독립적")
                with col3:
                    avg_rate = filtered["jeonse_rate"].mean()
                    st.metric("평균 전세가율", f"{avg_rate:.1f}%")

                # 신축/구축 분리 차트
                st.markdown("---")
                st.markdown("#### 🏗️ 신축 vs 구축 비교")
                st.caption("신축: 평균 연식 10년 이하 | 구축: 평균 연식 10년 초과")

                # 신축/구축 분류
                new_regions = filtered[filtered["avg_building_age"] <= 10]
                old_regions = filtered[filtered["avg_building_age"] > 10]

                col_new, col_old = st.columns(2)

                with col_new:
                    st.markdown("##### 🆕 신축 지역")
                    if not new_regions.empty:
                        fig_new = px.scatter(
                            new_regions,
                            x="total_trades",
                            y="jeonse_rate",
                            size="total_households",
                            color="jeonse_rate",
                            color_continuous_scale=[
                                [0, "#4CAF50"],
                                [0.5, "#FFB74D"],
                                [1, "#E57373"],
                            ],
                            range_color=[40, 80],
                            hover_name="region",
                            labels={
                                "total_trades": "거래량",
                                "jeonse_rate": "전세가율(%)",
                            },
                        )
                        fig_new.update_layout(
                            height=300,
                            showlegend=False,
                            coloraxis_showscale=False,
                            title=f"신축 지역 ({len(new_regions)}개)",
                        )
                        fig_new.add_hline(y=70, line_dash="dash", line_color="#FF6B6B", line_width=1)
                        st.plotly_chart(fig_new, use_container_width=True)

                        avg_new = new_regions["jeonse_rate"].mean()
                        avg_trades_new = new_regions["total_trades"].mean()
                        st.metric(
                            "평균 전세가율",
                            f"{avg_new:.1f}%",
                            f"평균 거래량: {avg_trades_new:.0f}건",
                        )
                    else:
                        st.info("신축 지역 데이터가 없습니다.")

                with col_old:
                    st.markdown("##### 🏚️ 구축 지역")
                    if not old_regions.empty:
                        fig_old = px.scatter(
                            old_regions,
                            x="total_trades",
                            y="jeonse_rate",
                            size="total_households",
                            color="jeonse_rate",
                            color_continuous_scale=[
                                [0, "#4CAF50"],
                                [0.5, "#FFB74D"],
                                [1, "#E57373"],
                            ],
                            range_color=[40, 80],
                            hover_name="region",
                            labels={
                                "total_trades": "거래량",
                                "jeonse_rate": "전세가율(%)",
                            },
                        )
                        fig_old.update_layout(
                            height=300,
                            showlegend=False,
                            coloraxis_showscale=False,
                            title=f"구축 지역 ({len(old_regions)}개)",
                        )
                        fig_old.add_hline(y=70, line_dash="dash", line_color="#FF6B6B", line_width=1)
                        st.plotly_chart(fig_old, use_container_width=True)

                        avg_old = old_regions["jeonse_rate"].mean()
                        avg_trades_old = old_regions["total_trades"].mean()
                        st.metric(
                            "평균 전세가율",
                            f"{avg_old:.1f}%",
                            f"평균 거래량: {avg_trades_old:.0f}건",
                        )
                    else:
                        st.info("구축 지역 데이터가 없습니다.")

                # 신축 vs 구축 비교 요약
                if not new_regions.empty and not old_regions.empty:
                    diff_rate = new_regions["jeonse_rate"].mean() - old_regions["jeonse_rate"].mean()
                    diff_trades = new_regions["total_trades"].mean() - old_regions["total_trades"].mean()

                    st.markdown("---")
                    col1, col2 = st.columns(2)
                    with col1:
                        if diff_rate > 0:
                            st.info(f"📊 신축이 구축보다 전세가율 **{abs(diff_rate):.1f}%p 높음**")
                        else:
                            st.info(f"📊 구축이 신축보다 전세가율 **{abs(diff_rate):.1f}%p 높음**")
                    with col2:
                        if diff_trades > 0:
                            st.info(f"📈 신축이 구축보다 평균 거래량 **{abs(diff_trades):.0f}건 많음**")
                        else:
                            st.info(f"📈 구축이 신축보다 평균 거래량 **{abs(diff_trades):.0f}건 많음**")

            else:
                st.warning("필터 조건에 맞는 데이터가 없습니다.")

        else:
            st.warning("데이터가 없습니다.")

    except Exception as e:
        st.error(f"데이터 로딩 오류: {e}")

# --- Tab 3: 공급 규모 ---
with tab3:
    st.markdown("### 🏠 동별 공급 규모")
    st.caption("아파트 단지 수 및 총 세대수 기준")

    try:
        supply_df = load_region_supply()

        if not supply_df.empty:
            # KPI 카드
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("총 지역(동) 수", f"{len(supply_df)}개")
            with col2:
                st.metric("총 아파트 단지", f"{supply_df['apt_count'].sum():,}개")
            with col3:
                st.metric("총 세대수", f"{supply_df['total_households'].sum():,.0f}세대")
            with col4:
                avg_age = supply_df["avg_building_age"].mean()
                st.metric("평균 연식", f"{avg_age:.1f}년")

            st.markdown("---")

            # 차트 2개
            col1, col2 = st.columns(2)

            with col1:
                fig1 = create_supply_bar_chart(supply_df)
                st.plotly_chart(fig1, use_container_width=True)

            with col2:
                fig2 = create_new_old_ratio_chart(supply_df)
                st.plotly_chart(fig2, use_container_width=True)

            # 상세 테이블
            with st.expander(f"📋 전체 지역 목록 ({len(supply_df)}개 동)"):
                display_df = supply_df.copy()
                display_df["avg_building_age"] = display_df["avg_building_age"].round(1)
                display_df.columns = ["지역", "단지수", "총세대수", "평균연식", "신축단지", "구축단지"]
                st.dataframe(display_df, use_container_width=True, hide_index=True)

        else:
            st.warning("공급 데이터가 없습니다.")

    except Exception as e:
        st.error(f"데이터 로딩 오류: {e}")

# --- Tab 4: 신규 입주 예정 ---
with tab4:
    st.markdown("### 🆕 신규 입주 예정 단지")
    st.caption("building_age < 0인 미준공/분양권 단지")

    try:
        upcoming_df = load_upcoming_supply()

        if not upcoming_df.empty:
            # KPI
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("입주 예정 단지", f"{len(upcoming_df)}개")
            with col2:
                st.metric("입주 예정 세대", f"{upcoming_df['total_households'].sum():,.0f}세대")
            with col3:
                regions_count = upcoming_df["region"].nunique()
                st.metric("관련 지역", f"{regions_count}개 동")

            st.markdown("---")

            # 동별 입주 예정 세대수
            region_upcoming = (
                upcoming_df.groupby("region").agg({"apartment_name": "count", "total_households": "sum"}).reset_index()
            )
            region_upcoming.columns = ["region", "apt_count", "total_households"]
            region_upcoming = region_upcoming.sort_values("total_households", ascending=False)

            fig = px.bar(
                region_upcoming.head(10),
                x="region",
                y="total_households",
                color="apt_count",
                title="동별 입주 예정 세대수 (상위 10개 동)",
                labels={"region": "지역", "total_households": "입주 예정 세대수", "apt_count": "단지 수"},
                text="total_households",
                color_continuous_scale="Blues",
            )
            fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

            # 상세 리스트
            st.markdown("#### 📋 입주 예정 단지 목록")
            display_df = upcoming_df.copy()
            display_df["construction_year"] = display_df["construction_year"].fillna(0).astype(int)
            display_df["입주까지"] = display_df["building_age"].abs().astype(str) + "년"
            display_df = display_df[["region", "apartment_name", "construction_year", "total_households", "입주까지"]]
            display_df.columns = ["지역", "아파트명", "준공예정년도", "세대수", "입주까지"]
            st.dataframe(display_df, use_container_width=True, hide_index=True)

        else:
            st.info("📭 현재 입주 예정 단지가 없습니다.")

    except Exception as e:
        st.error(f"데이터 로딩 오류: {e}")
