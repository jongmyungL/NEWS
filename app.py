import os
import re
import uuid
from datetime import datetime, timedelta
from html import unescape
from io import BytesIO
from typing import Dict, List, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh


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
    if "keywords" not in st.session_state:
        st.session_state.keywords = ["삼성화재"]

    if "folders" not in st.session_state:
        st.session_state.folders = ["보도자료", "기획기사", "위기관리", "경쟁사 동향"]

    if "inbox_articles" not in st.session_state:
        st.session_state.inbox_articles = []

    if "saved_articles" not in st.session_state:
        st.session_state.saved_articles = []

    if "correction_items" not in st.session_state:
        st.session_state.correction_items = []

    if "alerts" not in st.session_state:
        st.session_state.alerts = []

    if "auto_collect_enabled" not in st.session_state:
        st.session_state.auto_collect_enabled = True

    if "last_auto_collect_at" not in st.session_state:
        st.session_state.last_auto_collect_at = datetime.now()


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


def naver_api_ready() -> bool:
    load_dotenv()
    client_id = os.getenv("NAVER_CLIENT_ID", "")
    client_secret = os.getenv("NAVER_CLIENT_SECRET", "")
    return bool(client_id and client_secret)


def clean_html(text: str) -> str:
    no_tags = re.sub(r"<[^>]+>", "", text or "")
    return unescape(no_tags).strip()


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


def collect_news_from_naver() -> Tuple[int, str]:
    load_dotenv()
    client_id = os.getenv("NAVER_CLIENT_ID", "")
    client_secret = os.getenv("NAVER_CLIENT_SECRET", "")

    if not (client_id and client_secret):
        return 0, "no_key"

    endpoint = "https://openapi.naver.com/v1/search/news.json"
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
    }
    keywords = st.session_state.keywords or ["삼성화재"]
    existing_links = {a["link"] for a in st.session_state.inbox_articles}
    added = 0

    try:
        for keyword in keywords:
            params = {
                "query": keyword,
                "display": 20,
                "start": 1,
                "sort": "date",
            }
            response = requests.get(endpoint, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            payload = response.json()
            items = payload.get("items", [])

            for item in items:
                link = item.get("originallink") or item.get("link") or ""
                if not link or link in existing_links:
                    continue

                title = clean_html(item.get("title", "제목 없음"))
                summary = clean_html(item.get("description", ""))
                press_raw = clean_html(item.get("source", ""))
                press = normalize_press_name(press_raw, link)
                published_at = parse_naver_pub_date(item.get("pubDate", ""))

                article = make_article(
                    title=title,
                    press=press,
                    published_at=published_at,
                    link=link,
                    summary=summary,
                    query_keyword=keyword,
                )
                st.session_state.inbox_articles.insert(0, article)
                existing_links.add(link)
                added += 1
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

    st.sidebar.divider()
    if naver_api_ready():
        st.sidebar.success("네이버 API 키가 설정되어 있습니다.")
    else:
        st.sidebar.warning(
            "네이버 API 키가 아직 없습니다.\n`.env`에 NAVER_CLIENT_ID / NAVER_CLIENT_SECRET를 설정하세요."
        )

    if st.sidebar.button("지금 뉴스 수집 실행"):
        added_count, source = collect_news_from_naver()
        refresh_alerts()
        if source == "api":
            st.sidebar.success(f"수집 완료: 네이버 API 기사 {added_count}건 추가")
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

    target_folder = st.selectbox("저장할 섹션(폴더) 선택", st.session_state.folders)

    inbox_sorted = sorted(
        st.session_state.inbox_articles,
        key=lambda x: x["published_at"],
        reverse=True,
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
                            }
                        )
                        saved_count += 1
                st.success(f"{saved_count}건을 '{target_folder}' 폴더에 저장했습니다.")


def page_saved_db() -> None:
    st.title("스크랩 DB 및 폴더 관리")
    st.caption("영구 저장된 기사와 폴더를 관리하고 엑셀로 내보낼 수 있습니다.")

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

    if not saved:
        st.info("저장된 기사가 없습니다.")
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

    excel_export_df = display_df.drop(columns=["선택", "_saved_id"])
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
    elif page == "스크랩 DB 및 폴더 관리":
        page_saved_db()
    elif page == "기사 수정 요청 관리":
        page_correction_tracking()


if __name__ == "__main__":
    main()
