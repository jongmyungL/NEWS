import json
import os
import re
import uuid
from collections.abc import Mapping
from datetime import datetime, timedelta
from html import unescape
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh

try:
    import gspread
    from google.oauth2.service_account import Credentials
    _SHEETS_AVAILABLE = True
except ImportError:
    gspread = None
    _SHEETS_AVAILABLE = False


st.set_page_config(
    page_title="PR-Radar | 자사 뉴스 모니터링",
    page_icon="🛰️",
    layout="wide",
)


NEGATIVE_KEYWORDS = ["논란", "소송", "구설", "불매", "갑질", "사과문"]
PRESS_NAME_MAP = {
    "yna": "연합뉴스",
    "yonhap": "연합뉴스",
    "mk": "매일경제",
    "hankyung": "한국경제",
    "mt": "머니투데이",
    "sedaily": "서울경제",
    "etnews": "전자신문",
    "heraldcorp": "헤럴드경제",
    "chosunbiz": "조선비즈",
    "asiae": "아시아경제",
}

SHEET_NAMES = ("inbox", "saved", "corrections", "config", "keyword_results")


def _dt_to_iso(dt: datetime) -> str:
    return dt.isoformat() if isinstance(dt, datetime) else str(dt)


def _iso_to_dt(s: str) -> datetime:
    if not s:
        return datetime.now()
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return datetime.now()


def _get_sheets_credentials() -> Optional[Dict[str, Any]]:
    """서비스 계정 인증 정보 반환. st.secrets 또는 .env."""
    if not _SHEETS_AVAILABLE:
        return None
    load_dotenv()

    def from_section(section: Any) -> Optional[Dict[str, Any]]:
        if section is None:
            return None
        try:
            if isinstance(section, Mapping):
                d = dict(section)
            elif hasattr(section, "to_dict"):
                d = section.to_dict()
            else:
                d = dict(section) if hasattr(section, "items") else {}
            if not d.get("private_key"):
                return None
            return {
                "type": d.get("type", "service_account"),
                "project_id": d.get("project_id", ""),
                "private_key_id": d.get("private_key_id", ""),
                "private_key": d.get("private_key", ""),
                "client_email": d.get("client_email", ""),
                "client_id": d.get("client_id", ""),
                "auth_uri": d.get("auth_uri", "https://accounts.google.com/o/oauth2/auth"),
                "token_uri": d.get("token_uri", "https://oauth2.googleapis.com/token"),
                "auth_provider_x509_cert_url": d.get("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs"),
                "client_x509_cert_url": d.get("client_x509_cert_url", ""),
            }
        except Exception:
            return None

    # 1) [gcp_credentials] 섹션 (Streamlit Cloud에서 권장 - 여러 줄로 넣기 쉬움)
    try:
        section = None
        if hasattr(st.secrets, "get"):
            section = st.secrets.get("gcp_credentials")
        if section is None and hasattr(st.secrets, "gcp_credentials"):
            section = getattr(st.secrets, "gcp_credentials", None)
        if hasattr(st.secrets, "to_dict"):
            section = section or st.secrets.to_dict().get("gcp_credentials")
        out = from_section(section)
        if out:
            return out
    except Exception:
        pass

    # 2) gcp_credentials_json 한 줄
    try:
        raw = ""
        if hasattr(st.secrets, "get"):
            raw = st.secrets.get("gcp_credentials_json", "") or ""
        if not raw and hasattr(st.secrets, "gcp_credentials_json"):
            raw = getattr(st.secrets, "gcp_credentials_json", "") or ""
        if hasattr(st.secrets, "to_dict"):
            raw = raw or st.secrets.to_dict().get("gcp_credentials_json", "")
        raw = (raw or "").strip().strip("'").strip('"')
        if raw:
            if not raw.startswith("{"):
                raw = "{" + raw
            if not raw.rstrip().endswith("}"):
                raw = raw.rstrip() + "}"
            return json.loads(raw)
    except Exception:
        pass
    return None


def _get_sheet_id() -> str:
    load_dotenv()
    sid = (
        os.getenv("GOOGLE_SHEET_ID", "")
        or os.getenv("google_sheet_id", "")
        or (st.secrets.get("google_sheet_id", "") if hasattr(st.secrets, "get") else "")
        or getattr(st.secrets, "google_sheet_id", "")
        or ""
    )
    return str(sid).strip()


def _open_spreadsheet():
    """스프레드시트 열기. 실패 시 None."""
    if not _SHEETS_AVAILABLE:
        return None
    cred_dict = _get_sheets_credentials()
    sheet_id = _get_sheet_id()
    if not cred_dict or not sheet_id:
        return None
    try:
        creds = Credentials.from_service_account_info(cred_dict, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ])
        gc = gspread.authorize(creds)
        return gc.open_by_key(sheet_id)
    except Exception:
        return None


def _ensure_worksheets(sh: Any) -> None:
    """필요한 시트가 없으면 생성."""
    if not sh:
        return
    try:
        existing = [ws.title for ws in sh.worksheets()]
        for name in SHEET_NAMES:
            if name not in existing:
                try:
                    sh.add_worksheet(title=name, rows=500, cols=20)
                except Exception:
                    pass
    except Exception:
        pass


def _ensure_keyword_results_worksheet(sh: Any) -> bool:
    """keyword_results 시트가 없으면 생성 후 True, 이미 있으면 True, 실패 시 False."""
    if not sh:
        return False
    try:
        sh.worksheet("keyword_results")
        return True
    except Exception:
        try:
            existing = [ws.title for ws in sh.worksheets()]
            if "keyword_results" not in existing:
                sh.add_worksheet(title="keyword_results", rows=500, cols=20)
            return True
        except Exception:
            return False


def load_all_from_sheets() -> Optional[Dict[str, Any]]:
    """스프레드시트에서 전체 데이터 로드. 실패 시 None."""
    sh = _open_spreadsheet()
    if not sh:
        return None
    try:
        _ensure_worksheets(sh)
        out = {
            "inbox_articles": [],
            "saved_articles": [],
            "correction_items": [],
            "keywords": ["삼성화재"],
            "folders": ["보도자료", "기획기사", "위기관리", "경쟁사 동향"],
            "keyword_search_results": [],
        }
        # inbox
        try:
            ws = sh.worksheet("inbox")
            rows = ws.get_all_records()
            for r in rows:
                if not r.get("id"):
                    continue
                out["inbox_articles"].append({
                    "id": r.get("id", ""),
                    "title": r.get("title", ""),
                    "press": r.get("press", ""),
                    "published_at": _iso_to_dt(r.get("published_at", "")),
                    "link": r.get("link", ""),
                    "summary": r.get("summary", ""),
                    "query_keyword": r.get("query_keyword", ""),
                    "is_negative": str(r.get("is_negative", "")).lower() in ("true", "1", "yes"),
                    "negative_hits": r.get("negative_hits", ""),
                    "collected_at": _iso_to_dt(r.get("collected_at", "")),
                })
        except Exception:
            pass
        # saved
        try:
            ws = sh.worksheet("saved")
            rows = ws.get_all_records()
            for r in rows:
                if not r.get("saved_id"):
                    continue
                out["saved_articles"].append({
                    "saved_id": r.get("saved_id", ""),
                    "article_id": r.get("article_id", ""),
                    "folder": r.get("folder", ""),
                    "saved_at": _iso_to_dt(r.get("saved_at", "")),
                    "title": r.get("title", ""),
                    "press": r.get("press", ""),
                    "published_at": _iso_to_dt(r.get("published_at", "")),
                    "link": r.get("link", ""),
                    "summary": r.get("summary", ""),
                    "negative_hits": r.get("negative_hits", ""),
                    "summary_50": r.get("summary_50", ""),
                })
        except Exception:
            pass
        # corrections
        try:
            ws = sh.worksheet("corrections")
            rows = ws.get_all_records()
            for r in rows:
                if not r.get("id"):
                    continue
                out["correction_items"].append({
                    "id": r.get("id", ""),
                    "article_id": r.get("article_id", ""),
                    "published_at": _iso_to_dt(r.get("published_at", "")),
                    "press": r.get("press", ""),
                    "title": r.get("title", ""),
                    "link": r.get("link", ""),
                    "status": r.get("status", "요청됨"),
                    "memo": r.get("memo", ""),
                })
        except Exception:
            pass
        # config: keywords, folders
        try:
            ws = sh.worksheet("config")
            rows = ws.get_all_records()
            for r in rows:
                k, v = r.get("key", ""), r.get("value", "")
                if k == "keywords" and v:
                    out["keywords"] = [x.strip() for x in v.split(",") if x.strip()] or ["삼성화재"]
                elif k == "folders" and v:
                    out["folders"] = [x.strip() for x in v.split(",") if x.strip()] or out["folders"]
        except Exception:
            pass
        # keyword_search_results (시트 없으면 생성 후 로드)
        try:
            _ensure_keyword_results_worksheet(sh)
            ws = sh.worksheet("keyword_results")
            rows = ws.get_all_records()
            for r in rows:
                if not r.get("id"):
                    continue
                out["keyword_search_results"].append({
                    "id": r.get("id", ""),
                    "title": r.get("title", ""),
                    "press": r.get("press", ""),
                    "published_at": _iso_to_dt(r.get("published_at", "")),
                    "link": r.get("link", ""),
                    "summary": r.get("summary", ""),
                    "query_keyword": r.get("query_keyword", ""),
                    "is_negative": str(r.get("is_negative", "")).lower() in ("true", "1", "yes"),
                    "negative_hits": r.get("negative_hits", ""),
                    "collected_at": _iso_to_dt(r.get("collected_at", "")),
                })
        except Exception:
            pass
        return out
    except Exception:
        return None


def save_keyword_search_results_to_sheets() -> bool:
    """키워드 검색결과만 구글 시트에 저장. 성공 여부 반환. (수집 직후 즉시 호출용)"""
    if not _SHEETS_AVAILABLE:
        return False
    sh = _open_spreadsheet()
    if not sh:
        return False
    if not _ensure_keyword_results_worksheet(sh):
        return False
    try:
        ws = sh.worksheet("keyword_results")
        ws.clear()
        rows = [["id", "title", "press", "published_at", "link", "summary", "query_keyword", "is_negative", "negative_hits", "collected_at"]]
        for a in st.session_state.get("keyword_search_results", []):
            rows.append([
                a.get("id", ""),
                a.get("title", ""),
                a.get("press", ""),
                _dt_to_iso(a.get("published_at")),
                a.get("link", ""),
                a.get("summary", ""),
                a.get("query_keyword", ""),
                str(a.get("is_negative", False)),
                a.get("negative_hits", ""),
                _dt_to_iso(a.get("collected_at")),
            ])
        ws.update(rows, "A1")
        return True
    except Exception:
        return False


def save_all_to_sheets() -> bool:
    """현재 session_state 데이터를 스프레드시트에 저장. 성공 여부 반환."""
    if not _SHEETS_AVAILABLE:
        return False
    sh = _open_spreadsheet()
    if not sh:
        return False
    try:
        _ensure_worksheets(sh)
        # 키워드 검색결과를 가장 먼저 저장 (다른 시트 저장 실패해도 유지되도록)
        save_keyword_search_results_to_sheets()
        # inbox
        ws = sh.worksheet("inbox")
        ws.clear()
        rows = [["id", "title", "press", "published_at", "link", "summary", "query_keyword", "is_negative", "negative_hits", "collected_at"]]
        for a in st.session_state.get("inbox_articles", []):
            rows.append([
                a.get("id", ""),
                a.get("title", ""),
                a.get("press", ""),
                _dt_to_iso(a.get("published_at")),
                a.get("link", ""),
                a.get("summary", ""),
                a.get("query_keyword", ""),
                str(a.get("is_negative", False)),
                a.get("negative_hits", ""),
                _dt_to_iso(a.get("collected_at")),
            ])
        ws.update(rows, "A1")
        # saved
        ws = sh.worksheet("saved")
        ws.clear()
        rows = [["saved_id", "article_id", "folder", "saved_at", "title", "press", "published_at", "link", "summary", "negative_hits", "summary_50"]]
        for a in st.session_state.get("saved_articles", []):
            rows.append([
                a.get("saved_id", ""),
                a.get("article_id", ""),
                a.get("folder", ""),
                _dt_to_iso(a.get("saved_at")),
                a.get("title", ""),
                a.get("press", ""),
                _dt_to_iso(a.get("published_at")),
                a.get("link", ""),
                a.get("summary", ""),
                a.get("negative_hits", ""),
                a.get("summary_50", ""),
            ])
        ws.update(rows, "A1")
        # corrections
        ws = sh.worksheet("corrections")
        ws.clear()
        rows = [["id", "article_id", "published_at", "press", "title", "link", "status", "memo"]]
        for a in st.session_state.get("correction_items", []):
            rows.append([
                a.get("id", ""),
                a.get("article_id", ""),
                _dt_to_iso(a.get("published_at")),
                a.get("press", ""),
                a.get("title", ""),
                a.get("link", ""),
                a.get("status", ""),
                a.get("memo", ""),
            ])
        ws.update(rows, "A1")
        # config
        ws = sh.worksheet("config")
        ws.clear()
        ws.update([["key", "value"], ["keywords", ",".join(st.session_state.get("keywords", ["삼성화재"]))], ["folders", ",".join(st.session_state.get("folders", []))]], "A1")
        return True
    except Exception:
        return False


def sheets_ready() -> bool:
    """Google Sheets 연동 가능 여부."""
    return bool(_SHEETS_AVAILABLE and _get_sheets_credentials() and _get_sheet_id())


def parse_dt(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M")


def fmt_dt(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M")


def make_article(
    title: str,
    press: str,
    published_at: datetime,
    link: str,
    summary: str,
    query_keyword: str,
) -> Dict:
    hit_keywords = [kw for kw in NEGATIVE_KEYWORDS if kw in title or kw in summary]
    return {
        "id": str(uuid.uuid4())[:8],
        "title": title,
        "press": press,
        "published_at": published_at,
        "link": link,
        "summary": summary,
        "query_keyword": query_keyword,
        "is_negative": len(hit_keywords) > 0,
        "negative_hits": ", ".join(hit_keywords) if hit_keywords else "",
        "collected_at": datetime.now(),
    }


def get_mock_articles() -> List[Dict]:
    now = datetime.now()
    samples = [
        make_article(
            "OO테크, 신제품 출시로 해외 시장 공략 본격화",
            "매일경제",
            now - timedelta(hours=1),
            "https://example.com/news/1",
            "OO테크가 AI 기반 신제품을 공개하며 글로벌 확장 전략을 발표했다.",
            "OO테크",
        ),
        make_article(
            "OO테크 임원 인터뷰: \"올해 매출 2배 목표\"",
            "한국경제",
            now - timedelta(hours=3),
            "https://example.com/news/2",
            "핵심 임원이 사업 계획과 신규 투자 방향을 설명했다.",
            "홍길동",
        ),
        make_article(
            "OO테크 협력사와 소송 가능성 제기... 업계 긴장",
            "머니투데이",
            now - timedelta(hours=5),
            "https://example.com/news/3",
            "계약 해석을 둘러싼 갈등으로 소송 가능성이 언급됐다.",
            "OO테크",
        ),
        make_article(
            "소비자 커뮤니티서 OO테크 서비스 품질 논란 확산",
            "연합뉴스",
            now - timedelta(hours=7),
            "https://example.com/news/4",
            "일부 사용자 불만이 커뮤니티를 중심으로 빠르게 확산 중이다.",
            "OO테크",
        ),
        make_article(
            "OO테크, 대학과 산학 협력 프로젝트 추진",
            "전자신문",
            now - timedelta(days=1, hours=2),
            "https://example.com/news/5",
            "인재 양성과 연구개발 협력을 위한 장기 프로그램이 시작됐다.",
            "OO테크",
        ),
        make_article(
            "OO테크, 지속가능경영 보고서 공개",
            "서울경제",
            now - timedelta(days=2),
            "https://example.com/news/6",
            "환경·사회·지배구조 성과를 담은 보고서를 발표했다.",
            "OO테크",
        ),
        make_article(
            "OO테크 관련 구설 해명... 공식 사과문 게재",
            "조선비즈",
            now - timedelta(days=3, hours=4),
            "https://example.com/news/7",
            "회사 측은 사실관계를 설명하고 재발 방지책을 밝혔다.",
            "OO테크",
        ),
        make_article(
            "OO테크, 신입 공개채용 시작",
            "헤럴드경제",
            now - timedelta(days=6, hours=1),
            "https://example.com/news/8",
            "개발·기획 등 여러 직군에서 대규모 채용을 진행한다.",
            "OO테크",
        ),
    ]
    return samples


def init_state() -> None:
    # 첫 로드 시 Google Sheets에서 복원 시도
    if "inbox_articles" not in st.session_state:
        data = load_all_from_sheets() if sheets_ready() else None
        if data:
            st.session_state.keywords = data.get("keywords", ["삼성화재"])
            st.session_state.folders = data.get("folders", ["보도자료", "기획기사", "위기관리", "경쟁사 동향"])
            st.session_state.inbox_articles = data.get("inbox_articles", [])
            st.session_state.saved_articles = data.get("saved_articles", [])
            st.session_state.correction_items = data.get("correction_items", [])
            st.session_state.keyword_search_results = data.get("keyword_search_results", [])
        else:
            st.session_state.keywords = ["삼성화재"]
            st.session_state.folders = ["보도자료", "기획기사", "위기관리", "경쟁사 동향"]
            st.session_state.inbox_articles = []
            st.session_state.saved_articles = []
            st.session_state.correction_items = []
            st.session_state.keyword_search_results = []

    if "alerts" not in st.session_state:
        st.session_state.alerts = []

    if "auto_collect_enabled" not in st.session_state:
        st.session_state.auto_collect_enabled = True

    if "last_auto_collect_at" not in st.session_state:
        st.session_state.last_auto_collect_at = datetime.now()

    if "keyword_search_mode" not in st.session_state:
        st.session_state.keyword_search_mode = "OR"


def purge_old_inbox(days: int = 7) -> None:
    threshold = datetime.now() - timedelta(days=days)
    st.session_state.inbox_articles = [
        a for a in st.session_state.inbox_articles if a["collected_at"] >= threshold
    ]


def refresh_alerts() -> None:
    items = []
    for article in st.session_state.inbox_articles:
        if article["is_negative"]:
            items.append(
                {
                    "time": article["published_at"],
                    "message": f"[경고] 부정 키워드({article['negative_hits']}) 감지 - {article['title']}",
                }
            )
    st.session_state.alerts = sorted(items, key=lambda x: x["time"], reverse=True)


def _clean_secret_value(value: str) -> str:
    return str(value).strip().strip('"').strip("'")


def get_naver_credentials() -> Tuple[str, str]:
    load_dotenv()

    # 1) 환경변수(.env)
    env_client_id = _clean_secret_value(os.getenv("NAVER_CLIENT_ID", ""))
    env_client_secret = _clean_secret_value(os.getenv("NAVER_CLIENT_SECRET", ""))
    if env_client_id and env_client_secret:
        return env_client_id, env_client_secret

    # 2) Streamlit Secrets (루트 키 또는 섹션형 모두 지원)
    try:
        secrets_dict = {}
        if hasattr(st.secrets, "to_dict"):
            secrets_dict = st.secrets.to_dict()

        root_id = _clean_secret_value(
            st.secrets.get("NAVER_CLIENT_ID", "")
            or st.secrets.get("naver_client_id", "")
            or secrets_dict.get("NAVER_CLIENT_ID", "")
            or secrets_dict.get("naver_client_id", "")
        )
        root_secret = _clean_secret_value(
            st.secrets.get("NAVER_CLIENT_SECRET", "")
            or st.secrets.get("naver_client_secret", "")
            or secrets_dict.get("NAVER_CLIENT_SECRET", "")
            or secrets_dict.get("naver_client_secret", "")
        )
        if root_id and root_secret:
            return root_id, root_secret

        for section_name in ["naver", "NAVER", "api", "credentials"]:
            section = st.secrets.get(section_name, None)
            section_dict = {}

            if isinstance(section, Mapping):
                section_dict = dict(section)
            elif hasattr(section, "to_dict"):
                section_dict = section.to_dict()
            elif section_name in secrets_dict and isinstance(secrets_dict.get(section_name), Mapping):
                section_dict = dict(secrets_dict.get(section_name, {}))

            if section_dict:
                sec_id = _clean_secret_value(
                    section_dict.get("NAVER_CLIENT_ID", "")
                    or section_dict.get("naver_client_id", "")
                    or section_dict.get("client_id", "")
                )
                sec_secret = _clean_secret_value(
                    section_dict.get("NAVER_CLIENT_SECRET", "")
                    or section_dict.get("naver_client_secret", "")
                    or section_dict.get("client_secret", "")
                )
                if sec_id and sec_secret:
                    return sec_id, sec_secret
    except Exception:
        pass

    return "", ""


def naver_api_ready() -> bool:
    client_id, client_secret = get_naver_credentials()
    return bool(client_id and client_secret)


def clean_html(text: str) -> str:
    no_tags = re.sub(r"<[^>]+>", "", text or "")
    return unescape(no_tags).strip()


def truncate_to_50_words(text: str) -> str:
    """텍스트를 50단어 정도로 자른 요약 문자열 반환 (스크랩 시 저장, 엑셀 다운로드용)."""
    if not (text or "").strip():
        return ""
    words = (text or "").split()
    return " ".join(words[:50]).strip()


def parse_naver_pub_date(value: str) -> datetime:
    # 예: "Thu, 26 Feb 2026 09:30:00 +0900"
    try:
        return datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %z").replace(tzinfo=None)
    except ValueError:
        return datetime.now()


def guess_press_from_link(link: str) -> str:
    try:
        host = urlparse(link).netloc.lower().replace("www.", "")
        if not host:
            return "언론사 미상"
        parts = host.split(".")
        if len(parts) >= 2:
            return parts[-2]
        return host
    except Exception:
        return "언론사 미상"


def normalize_press_name(raw_press: str, link: str) -> str:
    value = (raw_press or "").strip()
    if value and re.search(r"[가-힣]", value):
        return value

    lower_value = value.lower().replace(" ", "")
    for key, ko_name in PRESS_NAME_MAP.items():
        if key in lower_value:
            return ko_name

    host_guess = guess_press_from_link(link)
    lower_host_guess = host_guess.lower().replace(" ", "")
    for key, ko_name in PRESS_NAME_MAP.items():
        if key in lower_host_guess:
            return ko_name

    if value:
        return value
    return host_guess


def collect_news_from_naver(
    start_date: Any = None,
    end_date: Any = None,
    keywords_override: Optional[List[str]] = None,
    target: str = "inbox",
) -> Tuple[int, str]:
    from datetime import date as date_cls

    client_id, client_secret = get_naver_credentials()
    if not (client_id and client_secret):
        return 0, "no_key"

    use_keyword_results = target == "keyword_search_results"
    if use_keyword_results and "keyword_search_results" not in st.session_state:
        st.session_state.keyword_search_results = []

    endpoint = "https://openapi.naver.com/v1/search/news.json"
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
    }
    keywords = (keywords_override if keywords_override else st.session_state.keywords) or ["삼성화재"]
    use_date_filter = start_date is not None and end_date is not None
    mode = (st.session_state.get("keyword_search_mode") or "OR").strip().upper() if use_date_filter else "OR"
    if mode != "AND":
        mode = "OR"
    start_d = start_date if isinstance(start_date, date_cls) else (start_date.date() if start_date and hasattr(start_date, "date") else None)
    end_d = end_date if isinstance(end_date, date_cls) else (end_date.date() if end_date and hasattr(end_date, "date") else None)
    use_date_filter = start_d is not None and end_d is not None

    target_list = st.session_state.keyword_search_results if use_keyword_results else st.session_state.inbox_articles
    existing_links = {a["link"] for a in target_list}
    added = 0
    # 임시보관함 수집(기간 미지정): 키워드당 최신 30건만. 기간 지정 수집: 100건씩 페이지네이션
    display_per_page = 100 if use_date_filter else 30
    max_start = 1001 - display_per_page

    def process_item(item: Dict, query_keyword: str) -> bool:
        """기사 처리. 날짜 필터 사용 시 start_d 이전 기사면 True(페이지 중단 신호)."""
        nonlocal added
        link = item.get("originallink") or item.get("link") or ""
        if not link or link in existing_links:
            return False
        pub_dt = parse_naver_pub_date(item.get("pubDate", ""))
        pub_d = pub_dt.date() if hasattr(pub_dt, "date") else pub_dt
        if use_date_filter and start_d and end_d:
            if pub_d < start_d:
                return True
            if pub_d > end_d:
                return False
        title = clean_html(item.get("title", "제목 없음"))
        summary = clean_html(item.get("description", ""))
        press_raw = clean_html(item.get("source", ""))
        press = normalize_press_name(press_raw, link)
        article = make_article(
            title=title,
            press=press,
            published_at=pub_dt,
            link=link,
            summary=summary,
            query_keyword=query_keyword,
        )
        target_list.insert(0, article)
        existing_links.add(link)
        added += 1
        return False

    try:
        if mode == "AND":
            query = " ".join(kw.strip() for kw in keywords if kw.strip())
            if not query:
                return 0, "api"
            start_idx = 1
            while start_idx <= max_start:
                params = {"query": query, "display": display_per_page, "start": start_idx, "sort": "date"}
                response = requests.get(endpoint, headers=headers, params=params, timeout=15)
                response.raise_for_status()
                items = response.json().get("items", [])
                if not items:
                    break
                stop_early = False
                for item in items:
                    if process_item(item, query):
                        stop_early = True
                        break
                if stop_early:
                    break
                start_idx += display_per_page
            return added, "api"
        else:
            # OR: 키워드별 검색. 임시보관함(기간 미지정)이면 키워드당 1페이지만(최신 30건), 기간 지정이면 페이지네이션
            for keyword in keywords:
                kw = (keyword or "").strip()
                if not kw:
                    continue
                start_idx = 1
                while start_idx <= max_start:
                    params = {"query": kw, "display": display_per_page, "start": start_idx, "sort": "date"}
                    response = requests.get(endpoint, headers=headers, params=params, timeout=15)
                    response.raise_for_status()
                    items = response.json().get("items", [])
                    if not items:
                        break
                    stop_early = False
                    for item in items:
                        if process_item(item, kw):
                            stop_early = True
                            break
                    if stop_early:
                        break
                    start_idx += display_per_page
                    if not use_date_filter:
                        break
            return added, "api"
    except requests.RequestException:
        return 0, "error"


def run_hourly_auto_collect() -> None:
    if not st.session_state.auto_collect_enabled:
        return
    if not naver_api_ready():
        return

    # 앱이 열려 있는 동안 1분마다 체크하고, 1시간 경과 시 자동 수집 실행
    st_autorefresh(interval=60 * 1000, key="hourly_auto_collect_tick")
    now = datetime.now()
    if now - st.session_state.last_auto_collect_at >= timedelta(hours=1):
        added_count, source = collect_news_from_naver()
        refresh_alerts()
        st.session_state.last_auto_collect_at = now
        if source == "api":
            st.toast(f"자동 수집 완료: {added_count}건", icon="⏱️")
        elif source == "error":
            st.toast("자동 수집 실패", icon="⚠️")


def to_excel_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    output.seek(0)
    return output.read()


def draw_sidebar() -> str:
    st.sidebar.title("PR-Radar")
    st.sidebar.caption("자사 뉴스 모니터링 및 DB 자동화")

    page = st.sidebar.radio(
        "메뉴",
        [
            "메인 대시보드",
            "임시 보관함 (Inbox)",
            "키워드 검색결과",
            "스크랩 DB 및 폴더 관리",
            "기사 수정 요청 관리",
        ],
    )

    st.sidebar.divider()
    with st.sidebar.expander("키워드 설정", expanded=False):
        st.caption("클릭했을 때만 펼쳐집니다. 여러 키워드를 등록해 수집할 수 있습니다.")

        new_keyword = st.text_input("새 키워드 추가", placeholder="예: CEO 이름")
        if st.button("키워드 추가"):
            cleaned = new_keyword.strip()
            if not cleaned:
                st.warning("추가할 키워드를 입력해 주세요.")
            elif cleaned in st.session_state.keywords:
                st.warning("이미 등록된 키워드입니다.")
            else:
                st.session_state.keywords.append(cleaned)
                st.success(f"'{cleaned}' 키워드를 추가했습니다.")

        if st.session_state.keywords:
            st.write("#### 등록된 키워드 (삭제할 항목 체크)")
            checked_to_delete = []
            for kw in st.session_state.keywords:
                key = f"delete_kw_{kw}"
                is_checked = st.checkbox(kw, key=key)
                if is_checked:
                    checked_to_delete.append(kw)

            if st.button("체크한 키워드 삭제"):
                if not checked_to_delete:
                    st.warning("삭제할 키워드를 체크해 주세요.")
                else:
                    st.session_state.keywords = [
                        kw for kw in st.session_state.keywords if kw not in checked_to_delete
                    ]
                    for kw in checked_to_delete:
                        st.session_state.pop(f"delete_kw_{kw}", None)
                    st.success(f"{len(checked_to_delete)}개 키워드를 삭제했습니다.")
        else:
            st.info("등록된 키워드가 없습니다.")

        st.sidebar.caption("등록된 키워드로 최근 뉴스를 수집해 임시보관함에 넣습니다. 키워드당 최신 30건, 누를 때마다 새 기사만 추가됩니다.")
        if st.sidebar.button("지금 뉴스 수집 (임시보관함)", key="btn_collect_inbox"):
            kw_list = [k for k in (st.session_state.keywords or []) if (k or "").strip()] or ["삼성화재"]
            added_count, source = collect_news_from_naver(start_date=None, end_date=None, keywords_override=kw_list, target="inbox")
            refresh_alerts()
            if source == "api":
                st.sidebar.success(f"수집 완료: 임시보관함에 {added_count}건 추가")
            elif source == "no_key":
                st.sidebar.warning("API 키가 없어 수집을 실행할 수 없습니다. `.env`를 확인해 주세요.")
            else:
                st.sidebar.warning("네이버 API 호출에 실패했습니다. 잠시 후 다시 시도해 주세요.")

    st.sidebar.divider()
    st.sidebar.write("### 수집 기간 (선택)")
    st.sidebar.caption("기간·키워드 지정 검색 시 [키워드 검색결과] 페이지로 수집 (최대 1000건)")
    use_date_range = st.sidebar.checkbox("기간 지정하여 수집", value=False, key="use_collect_date_range")
    collect_start_d = st.sidebar.date_input("기간 시작", value=datetime.now().date() - timedelta(days=7), key="collect_start")
    collect_end_d = st.sidebar.date_input("기간 끝", value=datetime.now().date(), key="collect_end")
    collect_keywords_override = None
    if use_date_range:
        all_kw = st.session_state.keywords or ["삼성화재"]
        if all_kw:
            default_sel = all_kw if len(all_kw) <= 10 else all_kw[:10]
            collect_keywords_override = st.sidebar.multiselect(
                "검색할 키워드 선택",
                all_kw,
                default=default_sel,
                key="collect_keywords_select",
            )
            mode_choice = st.sidebar.radio(
                "키워드 검색 방식",
                ["OR (선택 키워드별 각각 검색)", "AND (선택 키워드 모두 포함 검색)"],
                index=0 if (st.session_state.get("keyword_search_mode") or "OR").startswith("OR") else 1,
                key="collect_mode_radio",
            )
            st.session_state.keyword_search_mode = "AND" if "AND" in mode_choice else "OR"
    if use_date_range and collect_start_d > collect_end_d:
        st.sidebar.warning("기간 시작이 기간 끝보다 늦습니다. 기간 끝을 더 뒤로 설정하세요.")

    if use_date_range and st.sidebar.button("기간 지정하여 수집 실행", key="btn_collect_date_range"):
        start_d = collect_start_d
        end_d = collect_end_d
        kw_override = collect_keywords_override if collect_keywords_override else None
        if not kw_override:
            st.sidebar.warning("검색할 키워드를 하나 이상 선택해 주세요.")
        else:
            added_count, source = collect_news_from_naver(start_date=start_d, end_date=end_d, keywords_override=kw_override, target="keyword_search_results")
            refresh_alerts()
            if source == "api":
                if sheets_ready():
                    if save_keyword_search_results_to_sheets():
                        st.sidebar.caption("키워드 검색결과를 구글 시트에 저장했습니다.")
                    else:
                        st.sidebar.warning("키워드 검색결과 구글 시트 저장에 실패했습니다. 하단 'Google Sheets 연동 점검'을 확인해 주세요.")
                st.sidebar.success(f"수집 완료: 키워드 검색결과에 {added_count}건 추가")
            elif source == "no_key":
                st.sidebar.warning("API 키가 없어 수집을 실행할 수 없습니다. `.env`를 확인해 주세요.")
            else:
                st.sidebar.warning("네이버 API 호출에 실패했습니다. 잠시 후 다시 시도해 주세요.")

    if st.sidebar.button("임시 보관함 데이터 비우기"):
        st.session_state.inbox_articles = []
        st.session_state.alerts = []
        st.sidebar.success("임시 보관함을 비웠습니다.")

    st.sidebar.divider()
    st.sidebar.write("### 자동 수집 설정")
    st.session_state.auto_collect_enabled = st.sidebar.checkbox(
        "1시간마다 자동 수집",
        value=st.session_state.auto_collect_enabled,
    )
    if st.session_state.auto_collect_enabled:
        st.sidebar.caption(f"마지막 자동 수집: {fmt_dt(st.session_state.last_auto_collect_at)}")
    else:
        st.sidebar.caption("자동 수집이 꺼져 있습니다.")

    st.sidebar.divider()
    st.sidebar.write("---")
    st.sidebar.write("### 네이버 API")
    if naver_api_ready():
        st.sidebar.success("네이버 API 키 설정됨")
    else:
        st.sidebar.warning("네이버 API 키 없음. `.env` 또는 Secrets 확인.")
    if st.sidebar.button("API 키 인식 상태 점검", key="naver_check_btn"):
        cid, csec = get_naver_credentials()
        if cid and csec:
            st.sidebar.success("API 키 인식 성공")
        else:
            st.sidebar.error("API 키 인식 실패")

    st.sidebar.write("### Google Sheets DB")
    if sheets_ready():
        st.sidebar.success("Sheets 연동됨 (데이터 영구 보존)")
    else:
        st.sidebar.caption("Sheets 미연동 시 새로고침 시 데이터 초기화. GOOGLE_SHEETS_SETUP.md 참고.")
    if st.sidebar.button("Google Sheets 연동 점검", key="sheets_check_btn"):
        msgs = []
        sid = _get_sheet_id()
        if not sid:
            msgs.append("❌ google_sheet_id 없음")
        else:
            msgs.append(f"✅ 스프레드시트 ID 있음 ({sid[:8]}…)")
        cred = _get_sheets_credentials()
        if not cred:
            msgs.append("❌ gcp_credentials 없거나 파싱 실패")
        else:
            msgs.append("✅ 서비스 계정 로드됨")
        if sid and cred:
            try:
                sh = _open_spreadsheet()
                if sh:
                    msgs.append("✅ 스프레드시트 열기 성공")
                else:
                    msgs.append("❌ 스프레드시트 열기 실패 (편집자 공유 확인)")
            except Exception as e:
                msgs.append(f"❌ 오류: {e}")
        for m in msgs:
            st.sidebar.write(m)

    return page


def page_dashboard() -> None:
    st.title("메인 대시보드")
    st.caption("오늘의 뉴스 흐름과 위기 신호를 한눈에 확인하세요.")

    today = datetime.now().date()
    week_ago = datetime.now() - timedelta(days=7)

    collected_today = sum(
        1 for a in st.session_state.inbox_articles if a["published_at"].date() == today
    )
    scraped_this_week = sum(
        1 for a in st.session_state.saved_articles if a["saved_at"] >= week_ago
    )
    correction_in_progress = sum(
        1 for c in st.session_state.correction_items if c["status"] == "요청됨"
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("오늘 수집 기사 수", f"{collected_today}건")
    c2.metric("이번 주 스크랩 기사 수", f"{scraped_this_week}건")
    c3.metric("수정 요청 중", f"{correction_in_progress}건")

    st.divider()
    left, right = st.columns([1, 1.2])

    with left:
        st.subheader("최근 알림")
        if not st.session_state.alerts:
            st.info("부정 키워드 감지 알림이 없습니다.")
        else:
            for alert in st.session_state.alerts[:5]:
                st.warning(f"{fmt_dt(alert['time'])} | {alert['message']}")

    with right:
        st.subheader("최근 수집 기사")
        recent = sorted(
            st.session_state.inbox_articles,
            key=lambda x: x["published_at"],
            reverse=True,
        )[:5]
        if not recent:
            st.info("표시할 기사가 없습니다.")
        else:
            for a in recent:
                neg_tag = " 🚨" if a["is_negative"] else ""
                st.markdown(f"**[{a['title']}]({a['link']})**{neg_tag}")
                st.caption(f"{a['press']} | {fmt_dt(a['published_at'])} | 키워드: {a['query_keyword']}")


def page_inbox() -> None:
    st.title("임시 보관함 (Inbox)")
    st.caption("수집된 기사를 확인하고 필요한 기사만 영구 DB로 저장하세요. (7일 후 자동 삭제)")

    if not st.session_state.inbox_articles:
        st.info("임시 보관함에 기사가 없습니다.")
        return

    st.subheader("기간 필터 (발행일 기준)")
    col_a, col_b, col_c = st.columns([1, 1, 2])
    with col_a:
        filter_start = st.date_input("시작일 (YY.MM.DD)", value=datetime.now().date() - timedelta(days=30), key="inbox_start")
    with col_b:
        filter_end = st.date_input("종료일 (YY.MM.DD)", value=datetime.now().date(), key="inbox_end")
    with col_c:
        st.caption("해당 기간에 발행된 기사만 표시합니다.")
    if filter_start > filter_end:
        filter_start, filter_end = filter_end, filter_start

    target_folder = st.selectbox("저장할 섹션(폴더) 선택", st.session_state.folders)

    inbox_sorted = sorted(
        st.session_state.inbox_articles,
        key=lambda x: x["published_at"],
        reverse=True,
    )
    inbox_sorted = [
        a for a in inbox_sorted
        if (a["published_at"].date() if hasattr(a["published_at"], "date") else a["published_at"]) >= filter_start
        and (a["published_at"].date() if hasattr(a["published_at"], "date") else a["published_at"]) <= filter_end
    ]
    if not inbox_sorted:
        st.info("해당 기간에 해당하는 기사가 없습니다. 기간을 넓혀 보세요.")
        return

    inbox_export_df = pd.DataFrame([
        {
            "제목": a["title"],
            "언론사": normalize_press_name(a["press"], a["link"]),
            "일시": fmt_dt(a["published_at"]),
            "키워드": a["query_keyword"],
            "부정키워드": a["negative_hits"],
            "요약(50단어)": truncate_to_50_words(a.get("summary") or a.get("title", "")),
            "기사링크": a["link"],
        }
        for a in inbox_sorted
    ])
    st.download_button(
        label="엑셀 다운로드 (현재 필터 기준)",
        data=to_excel_bytes(inbox_export_df, "Inbox"),
        file_name=f"pr_radar_inbox_{datetime.now().strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="inbox_excel_dl",
    )

    registered_keywords = [k for k in st.session_state.keywords if k.strip()]
    tab_labels = ["전체"] + registered_keywords if registered_keywords else ["전체"]
    tabs = st.tabs(tab_labels)

    for idx, (tab, label) in enumerate(zip(tabs, tab_labels)):
        with tab:
            if label == "전체":
                filtered_articles = inbox_sorted
            else:
                filtered_articles = [a for a in inbox_sorted if a["query_keyword"] == label]

            if not filtered_articles:
                st.info(f"'{label}' 키워드 기사 없음")
                continue

            table_data = [
                {
                    "선택": False,
                    "제목": a["title"],
                    "언론사": normalize_press_name(a["press"], a["link"]),
                    "일시": fmt_dt(a["published_at"]),
                    "키워드": a["query_keyword"],
                    "부정키워드": a["negative_hits"],
                    "기사링크": a["link"],
                    "_id": a["id"],
                }
                for a in filtered_articles
            ]
            df = pd.DataFrame(table_data)
            current_editor = st.data_editor(
                df,
                hide_index=True,
                use_container_width=True,
                disabled=["제목", "일시", "키워드", "부정키워드", "기사링크", "_id"],
                column_config={
                    "_id": None,
                    "선택": st.column_config.CheckboxColumn("선택"),
                    "제목": st.column_config.TextColumn("제목", width="large"),
                    "기사링크": st.column_config.LinkColumn(
                        "기사 링크",
                    ),
                },
                key=f"inbox_editor_{idx}_{label}",
            )

            save_clicked = st.button(
                "선택한 기사 영구 저장하기",
                type="primary",
                key=f"save_inbox_{idx}_{label}",
            )
            if save_clicked:
                selected_ids = current_editor.loc[current_editor["선택"] == True, "_id"].tolist()
                if not selected_ids:
                    st.warning("저장할 기사를 먼저 선택해 주세요.")
                    continue

                saved_count = 0
                existing_ids = {a["article_id"] for a in st.session_state.saved_articles}
                edited_rows_by_id = {
                    row["_id"]: row.to_dict() for _, row in current_editor.iterrows()
                }
                for article in filtered_articles:
                    if article["id"] in selected_ids and article["id"] not in existing_ids:
                        edited_row = edited_rows_by_id.get(article["id"], {})
                        edited_press = str(edited_row.get("언론사", article["press"])).strip()
                        final_press = (
                            normalize_press_name(edited_press, article["link"])
                            if edited_press
                            else normalize_press_name(article["press"], article["link"])
                        )
                        article["press"] = final_press
                        summary_50 = truncate_to_50_words(article.get("summary") or article.get("title", ""))
                        st.session_state.saved_articles.append(
                            {
                                "saved_id": str(uuid.uuid4())[:8],
                                "article_id": article["id"],
                                "folder": target_folder,
                                "saved_at": datetime.now(),
                                "title": article["title"],
                                "press": final_press,
                                "published_at": article["published_at"],
                                "link": article["link"],
                                "summary": article["summary"],
                                "negative_hits": article["negative_hits"],
                                "summary_50": summary_50,
                            }
                        )
                        saved_count += 1
                st.success(f"{saved_count}건을 '{target_folder}' 폴더에 저장했습니다.")


def page_keyword_search_results() -> None:
    st.title("키워드 검색결과")
    st.caption("기간·키워드 지정 검색으로 수집된 기사입니다. 필요한 기사만 선택해 영구 DB로 저장할 수 있습니다.")
    if sheets_ready():
        st.caption("데이터는 구글 시트에 자동 저장됩니다. 새로고침 후에도 유지됩니다.")

    results = st.session_state.get("keyword_search_results", [])
    if not results:
        st.info("키워드 검색결과가 없습니다. 사이드바에서 '기간 지정하여 수집' 후 검색할 키워드를 선택하고 수집을 실행하세요.")
        return

    st.subheader("기간 필터 (발행일 기준)")
    col_a, col_b, col_c = st.columns([1, 1, 2])
    with col_a:
        kw_filter_start = st.date_input("시작일 (YY.MM.DD)", value=datetime.now().date() - timedelta(days=30), key="kw_start")
    with col_b:
        kw_filter_end = st.date_input("종료일 (YY.MM.DD)", value=datetime.now().date(), key="kw_end")
    with col_c:
        st.caption("해당 기간에 발행된 기사만 표시합니다.")
    if kw_filter_start > kw_filter_end:
        kw_filter_start, kw_filter_end = kw_filter_end, kw_filter_start

    target_folder = st.selectbox("저장할 섹션(폴더) 선택", st.session_state.folders, key="kw_target_folder")

    results_sorted = sorted(results, key=lambda x: x["published_at"], reverse=True)
    results_sorted = [
        a for a in results_sorted
        if (a["published_at"].date() if hasattr(a["published_at"], "date") else a["published_at"]) >= kw_filter_start
        and (a["published_at"].date() if hasattr(a["published_at"], "date") else a["published_at"]) <= kw_filter_end
    ]
    if not results_sorted:
        st.info("해당 기간에 해당하는 기사가 없습니다. 기간을 넓혀 보세요.")
        return

    def format_keyword_display(q: str) -> str:
        """AND 검색 시 'A B' -> 'A + B' 형태로 표시."""
        if not (q or "").strip():
            return ""
        return " + ".join((q or "").split())

    results_export_df = pd.DataFrame([
        {
            "제목": a["title"],
            "언론사": normalize_press_name(a["press"], a["link"]),
            "일시": fmt_dt(a["published_at"]),
            "키워드": format_keyword_display(a.get("query_keyword", "")),
            "부정키워드": a["negative_hits"],
            "요약(50단어)": truncate_to_50_words(a.get("summary") or a.get("title", "")),
            "기사링크": a["link"],
        }
        for a in results_sorted
    ])
    st.download_button(
        label="엑셀 다운로드 (현재 필터 기준)",
        data=to_excel_bytes(results_export_df, "Keyword_Search_Results"),
        file_name=f"pr_radar_keyword_results_{datetime.now().strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="keyword_results_excel_dl",
    )

    # AND 검색 시 query_keyword가 "A B" 형태이므로, 결과에 있는 query_keyword 기준으로 탭 구성
    unique_query_keywords = sorted(set((a.get("query_keyword") or "").strip() for a in results_sorted if (a.get("query_keyword") or "").strip()))
    tab_labels = ["전체"] + unique_query_keywords
    tabs = st.tabs(tab_labels)

    for idx, (tab, label) in enumerate(zip(tabs, tab_labels)):
        with tab:
            if label == "전체":
                filtered = results_sorted
            else:
                filtered = [a for a in results_sorted if (a.get("query_keyword") or "").strip() == label]
            if not filtered:
                st.info(f"'{format_keyword_display(label)}' 키워드 기사 없음")
                continue
            table_data = [
                {
                    "선택": False,
                    "제목": a["title"],
                    "언론사": normalize_press_name(a["press"], a["link"]),
                    "일시": fmt_dt(a["published_at"]),
                    "키워드": format_keyword_display(a.get("query_keyword", "")),
                    "부정키워드": a["negative_hits"],
                    "기사링크": a["link"],
                    "_id": a["id"],
                }
                for a in filtered
            ]
            df = pd.DataFrame(table_data)
            current_editor = st.data_editor(
                df,
                hide_index=True,
                use_container_width=True,
                disabled=["제목", "일시", "키워드", "부정키워드", "기사링크", "_id"],
                column_config={
                    "_id": None,
                    "선택": st.column_config.CheckboxColumn("선택"),
                    "제목": st.column_config.TextColumn("제목", width="large"),
                    "기사링크": st.column_config.LinkColumn("기사 링크"),
                },
                key=f"kw_editor_{idx}_{label}",
            )
            save_clicked = st.button("선택한 기사 영구 저장하기", type="primary", key=f"kw_save_{idx}_{label}")
            if save_clicked:
                selected_ids = current_editor.loc[current_editor["선택"] == True, "_id"].tolist()
                if not selected_ids:
                    st.warning("저장할 기사를 먼저 선택해 주세요.")
                    continue
                saved_count = 0
                existing_ids = {a["article_id"] for a in st.session_state.saved_articles}
                edited_rows_by_id = {row["_id"]: row.to_dict() for _, row in current_editor.iterrows()}
                for article in filtered:
                    if article["id"] in selected_ids and article["id"] not in existing_ids:
                        edited_row = edited_rows_by_id.get(article["id"], {})
                        edited_press = str(edited_row.get("언론사", article["press"])).strip()
                        final_press = (
                            normalize_press_name(edited_press, article["link"])
                            if edited_press
                            else normalize_press_name(article["press"], article["link"])
                        )
                        summary_50 = truncate_to_50_words(article.get("summary") or article.get("title", ""))
                        st.session_state.saved_articles.append({
                            "saved_id": str(uuid.uuid4())[:8],
                            "article_id": article["id"],
                            "folder": target_folder,
                            "saved_at": datetime.now(),
                            "title": article["title"],
                            "press": final_press,
                            "published_at": article["published_at"],
                            "link": article["link"],
                            "summary": article["summary"],
                            "negative_hits": article["negative_hits"],
                            "summary_50": summary_50,
                        })
                        saved_count += 1
                st.success(f"{saved_count}건을 '{target_folder}' 폴더에 저장했습니다.")

    col_save, col_clear = st.columns(2)
    with col_save:
        if sheets_ready() and st.button("지금 구글 시트에 저장", key="kw_manual_save"):
            if save_keyword_search_results_to_sheets():
                st.success("구글 시트에 저장했습니다.")
            else:
                st.error("저장에 실패했습니다. Google Sheets 연동을 확인해 주세요.")
    with col_clear:
        if st.button("키워드 검색결과 비우기", key="kw_clear_btn"):
            st.session_state.keyword_search_results = []
            if sheets_ready():
                try:
                    save_keyword_search_results_to_sheets()
                except Exception:
                    pass
            st.success("키워드 검색결과를 비웠습니다.")
            st.rerun()


def page_saved_db() -> None:
    st.title("스크랩 DB 및 폴더 관리")
    st.caption("영구 저장된 기사와 폴더를 관리하고 엑셀로 내보낼 수 있습니다.")

    st.subheader("기간 필터 (발행일 기준)")
    col_a, col_b, col_c = st.columns([1, 1, 2])
    with col_a:
        db_filter_start = st.date_input("시작일 (YY.MM.DD)", value=datetime.now().date() - timedelta(days=30), key="db_start")
    with col_b:
        db_filter_end = st.date_input("종료일 (YY.MM.DD)", value=datetime.now().date(), key="db_end")
    with col_c:
        st.caption("해당 기간에 발행된 스크랩 기사만 표시합니다.")
    if db_filter_start > db_filter_end:
        db_filter_start, db_filter_end = db_filter_end, db_filter_start

    st.subheader("폴더 관리")
    add_col, delete_col = st.columns(2)
    with add_col:
        new_folder = st.text_input("새 폴더명")
        if st.button("폴더 추가"):
            cleaned_folder = new_folder.strip()
            if not cleaned_folder:
                st.warning("폴더명을 입력해 주세요.")
            elif cleaned_folder in st.session_state.folders:
                st.warning("이미 존재하는 폴더입니다.")
            else:
                st.session_state.folders.append(cleaned_folder)
                st.success(f"'{cleaned_folder}' 폴더를 추가했습니다.")

    with delete_col:
        removable_folders = st.multiselect("삭제할 폴더(다중 선택)", st.session_state.folders)
        delete_with_articles = st.checkbox("해당 폴더 기사도 함께 삭제", value=False)
        if st.button("선택 폴더 삭제"):
            if not removable_folders:
                st.warning("삭제할 폴더를 선택해 주세요.")
            else:
                if delete_with_articles:
                    st.session_state.saved_articles = [
                        s for s in st.session_state.saved_articles if s["folder"] not in removable_folders
                    ]
                else:
                    fallback_folder = "미분류"
                    if fallback_folder not in st.session_state.folders:
                        st.session_state.folders.append(fallback_folder)
                    for saved in st.session_state.saved_articles:
                        if saved["folder"] in removable_folders:
                            saved["folder"] = fallback_folder

                st.session_state.folders = [
                    f for f in st.session_state.folders if f not in removable_folders
                ]
                if not st.session_state.folders:
                    st.session_state.folders = ["미분류"]
                st.success(f"{len(removable_folders)}개 폴더를 삭제했습니다.")

    st.divider()
    selected_folder = st.selectbox(
        "폴더 필터",
        ["전체"] + st.session_state.folders,
    )

    saved = st.session_state.saved_articles
    if selected_folder != "전체":
        saved = [s for s in saved if s["folder"] == selected_folder]
    saved = [
        s for s in saved
        if (s["published_at"].date() if hasattr(s["published_at"], "date") else s["published_at"]) >= db_filter_start
        and (s["published_at"].date() if hasattr(s["published_at"], "date") else s["published_at"]) <= db_filter_end
    ]

    if not saved:
        st.info("저장된 기사가 없거나 해당 기간에 해당하는 기사가 없습니다.")
        return

    display_df = pd.DataFrame(
        [
            {
                "선택": False,
                "폴더": s["folder"],
                "기사제목": s["title"],
                "언론사": normalize_press_name(s["press"], s["link"]),
                "발행일시": fmt_dt(s["published_at"]),
                "저장일시": fmt_dt(s["saved_at"]),
                "부정키워드": s["negative_hits"],
                "링크": s["link"],
                "_saved_id": s["saved_id"],
            }
            for s in sorted(saved, key=lambda x: x["saved_at"], reverse=True)
        ]
    )

    # 엑셀 다운로드용: 화면에 표시하지 않는 요약(50단어) 컬럼 포함
    excel_export_df = pd.DataFrame(
        [
            {
                "폴더": s["folder"],
                "기사제목": s["title"],
                "언론사": normalize_press_name(s["press"], s["link"]),
                "발행일시": fmt_dt(s["published_at"]),
                "저장일시": fmt_dt(s["saved_at"]),
                "부정키워드": s["negative_hits"],
                "요약(50단어)": s.get("summary_50", ""),
                "링크": s["link"],
            }
            for s in sorted(saved, key=lambda x: x["saved_at"], reverse=True)
        ]
    )
    excel_bytes = to_excel_bytes(excel_export_df, "Saved_DB")
    st.download_button(
        label="엑셀 다운로드",
        data=excel_bytes,
        file_name=f"pr_radar_saved_db_{datetime.now().strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    edited_saved_df = st.data_editor(
        display_df,
        hide_index=True,
        use_container_width=True,
        disabled=["폴더", "기사제목", "언론사", "발행일시", "저장일시", "부정키워드", "링크", "_saved_id"],
        column_config={
            "_saved_id": None,
            "선택": st.column_config.CheckboxColumn("선택"),
            "링크": st.column_config.LinkColumn("링크"),
        },
        key="saved_db_editor",
    )

    if st.button("선택한 스크랩 기사 삭제", type="secondary"):
        selected_saved_ids = edited_saved_df.loc[edited_saved_df["선택"] == True, "_saved_id"].tolist()
        if not selected_saved_ids:
            st.warning("삭제할 스크랩 기사를 선택해 주세요.")
        else:
            st.session_state.saved_articles = [
                s for s in st.session_state.saved_articles if s["saved_id"] not in selected_saved_ids
            ]
            st.success(f"{len(selected_saved_ids)}건의 스크랩 기사를 삭제했습니다.")

    st.divider()
    st.subheader("수정 요청 등록")
    options = {
        f"{s['title']} | {s['press']} | {fmt_dt(s['published_at'])}": s for s in saved
    }
    picked = st.selectbox("수정 요청할 기사 선택", list(options.keys()))
    memo = st.text_input("수정 요청 메모", placeholder="예: 제목 내 사실오류 정정 요청")
    if st.button("수정 요청 항목 추가"):
        chosen = options[picked]
        st.session_state.correction_items.append(
            {
                "id": str(uuid.uuid4())[:8],
                "article_id": chosen["article_id"],
                "published_at": chosen["published_at"],
                "press": chosen["press"],
                "title": chosen["title"],
                "link": chosen["link"],
                "status": "요청됨",
                "memo": memo,
            }
        )
        st.success("수정 요청 항목에 추가했습니다.")


def page_correction_tracking() -> None:
    st.title("기사 수정 요청 관리")
    st.caption("요청 상태와 수정 내용 메모를 업데이트하세요.")

    if not st.session_state.correction_items:
        st.info("등록된 수정 요청이 없습니다.")
        return

    for idx, item in enumerate(
        sorted(st.session_state.correction_items, key=lambda x: x["published_at"], reverse=True)
    ):
        with st.container(border=True):
            c1, c2 = st.columns([1, 3])
            c1.write(f"**발행일시**  \n{fmt_dt(item['published_at'])}")
            c1.write(f"**언론사**  \n{item['press']}")
            c2.markdown(f"**기사 제목:** [{item['title']}]({item['link']})")

            status = st.radio(
                "진행상태",
                ["요청됨", "수정완료", "확인불가"],
                horizontal=True,
                index=["요청됨", "수정완료", "확인불가"].index(item["status"]),
                key=f"status_{item['id']}_{idx}",
            )
            memo = st.text_input(
                "수정 내용 메모",
                value=item["memo"],
                key=f"memo_{item['id']}_{idx}",
            )

            item["status"] = status
            item["memo"] = memo

    df = pd.DataFrame(
        [
            {
                "발행일시": fmt_dt(i["published_at"]),
                "언론사": i["press"],
                "기사제목": i["title"],
                "링크": i["link"],
                "진행상태": i["status"],
                "수정내용메모": i["memo"],
            }
            for i in st.session_state.correction_items
        ]
    )
    excel_bytes = to_excel_bytes(df, "Corrections")
    st.download_button(
        label="수정 요청 내역 엑셀 다운로드",
        data=excel_bytes,
        file_name=f"pr_radar_corrections_{datetime.now().strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def main() -> None:
    init_state()
    run_hourly_auto_collect()
    purge_old_inbox(days=7)
    refresh_alerts()

    page = draw_sidebar()
    if page == "메인 대시보드":
        page_dashboard()
    elif page == "임시 보관함 (Inbox)":
        page_inbox()
    elif page == "키워드 검색결과":
        page_keyword_search_results()
    elif page == "스크랩 DB 및 폴더 관리":
        page_saved_db()
    elif page == "기사 수정 요청 관리":
        page_correction_tracking()

    # 매 실행마다 Google Sheets에 현재 상태 저장 (새로고침 후에도 유지)
    if sheets_ready():
        try:
            save_all_to_sheets()
        except Exception:
            pass


if __name__ == "__main__":
    main()
