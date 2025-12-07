"""BigQuery 클라이언트 및 공통 쿼리 함수"""
import os

import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account


@st.cache_resource
def get_bq_client():
    """BigQuery 클라이언트 생성 (캐시됨)

    - Streamlit Cloud: st.secrets에서 credential 로드
    - 로컬 개발: credential 파일 또는 .streamlit/secrets.toml 사용
    """
    # Streamlit Secrets 사용 (Cloud 배포용)
    if "gcp_service_account" in st.secrets:
        credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        return bigquery.Client(credentials=credentials, project=credentials.project_id)

    # 로컬 개발용: credential 파일 사용
    cred_path = "credential/ilovemyrealestate-27f37b5ebb2a.json"
    if os.path.exists(cred_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path
        return bigquery.Client()

    # 기본 (ADC - Application Default Credentials)
    return bigquery.Client()


# 프로젝트/데이터셋 정보
PROJECT_ID = "ilovemyrealestate"
DATASET_ID = "realestate_data"

# 테이블명
TABLE_MAEMAE = f"{PROJECT_ID}.{DATASET_ID}.maemae_history_latest"
TABLE_JEONSAE = f"{PROJECT_ID}.{DATASET_ID}.jeonsae_history_latest"
TABLE_COMPLEX = f"{PROJECT_ID}.{DATASET_ID}.complex_info_latest"
TABLE_AREA = f"{PROJECT_ID}.{DATASET_ID}.area_info_latest"

# 공통 필터 조건: 주상복합 제외
FILTER_EXCLUDE_JUSANGBOKHAP = "apartment_name NOT LIKE '%주상복합%'"
