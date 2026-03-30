"""Streamlit 页面样式和横幅组件。"""

import streamlit as st


def render_global_styles() -> None:
    """注入全局样式。"""

    st.markdown(
        """
        <style>
        :root {
          color-scheme: light;
        }
        .stApp {
          background:
            radial-gradient(circle at top right, rgba(14,165,233,0.14), transparent 24%),
            radial-gradient(circle at top left, rgba(16,185,129,0.10), transparent 26%),
            linear-gradient(180deg, #f8fbff 0%, #f7fafc 48%, #f8fafc 100%);
        }
        .block-container {
          max-width: 1380px;
          padding-top: 1.8rem;
          padding-bottom: 3rem;
          color: #0f172a !important;
        }
        .block-container h1,
        .block-container h2,
        .block-container h3,
        .block-container h4,
        .block-container h5,
        .block-container h6,
        .block-container p,
        .block-container label,
        .block-container li,
        .block-container span,
        .block-container strong,
        .block-container small,
        .block-container div[data-testid="stCaptionContainer"] p,
        .block-container div[data-testid="stMarkdownContainer"] p {
          color: #0f172a;
        }
        .block-container a {
          color: #0f5db8;
        }
        .block-container [data-testid="stVerticalBlockBorderWrapper"] {
          background: rgba(255,255,255,0.78);
          border-radius: 24px;
          border: 1px solid rgba(148, 163, 184, 0.15);
          box-shadow: 0 18px 38px rgba(15, 23, 42, 0.05);
          backdrop-filter: blur(8px);
        }
        .block-container [data-testid="stAlert"] {
          border-radius: 18px;
          border: 1px solid rgba(148, 163, 184, 0.14);
        }
        .block-container [data-testid="stAlert"] * {
          color: #0f172a !important;
        }
        .block-container [data-testid="stExpander"] {
          background: rgba(255,255,255,0.82);
          border-radius: 20px;
          border: 1px solid rgba(148, 163, 184, 0.12);
          overflow: hidden;
        }
        .block-container [data-testid="stDataFrame"],
        .block-container [data-testid="stTable"] {
          background: rgba(255,255,255,0.9);
          border-radius: 18px;
        }
        .block-container [data-testid="stDataFrame"] *,
        .block-container [data-testid="stTable"] * {
          color: #0f172a !important;
        }
        .block-container [data-baseweb="select"] > div,
        .block-container [data-baseweb="input"] > div,
        .block-container input,
        .block-container textarea {
          background: rgba(255,255,255,0.96) !important;
          color: #0f172a !important;
          border-color: rgba(148, 163, 184, 0.22) !important;
        }
        .block-container [data-baseweb="tag"] {
          background: #e2f3ff !important;
          color: #0f3d69 !important;
          border: 1px solid rgba(14, 94, 184, 0.15);
        }
        .block-container div[data-baseweb="select"] svg,
        .block-container div[data-baseweb="input"] svg {
          color: #475569 !important;
          fill: #475569 !important;
        }
        section[data-testid="stSidebar"] {
          background:
            radial-gradient(circle at top, rgba(56, 189, 248, 0.16), transparent 26%),
            linear-gradient(180deg, #09111f 0%, #0f172a 55%, #111827 100%);
          border-right: 1px solid rgba(255,255,255,0.08);
        }
        section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
        section[data-testid="stSidebar"] label,
        section[data-testid="stSidebar"] div,
        section[data-testid="stSidebar"] span {
          color: #e2e8f0;
        }
        section[data-testid="stSidebar"] [role="radiogroup"] {
          gap: 0.45rem;
        }
        section[data-testid="stSidebar"] [role="radiogroup"] label {
          background: rgba(255,255,255,0.06);
          border: 1px solid rgba(255,255,255,0.07);
          border-radius: 16px;
          padding: 0.75rem 0.8rem;
          margin-bottom: 0.55rem;
          transition: all 0.18s ease;
        }
        section[data-testid="stSidebar"] [role="radiogroup"] label:hover {
          background: rgba(255,255,255,0.10);
          border-color: rgba(125, 211, 252, 0.24);
        }
        section[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
          background: linear-gradient(135deg, rgba(14,165,233,0.24), rgba(16,185,129,0.18));
          border-color: rgba(125, 211, 252, 0.42);
          box-shadow: 0 16px 34px rgba(2, 132, 199, 0.18);
        }
        section[data-testid="stSidebar"] [role="radiogroup"] label p {
          font-size: 1.12rem !important;
          font-weight: 700 !important;
          letter-spacing: -0.01em;
          color: #f8fafc !important;
        }
        section[data-testid="stSidebar"] [data-testid="stSidebarNavSeparator"] {
          display: none;
        }
        div[data-testid="stMetric"] {
          background: rgba(255, 255, 255, 0.88);
          border: 1px solid rgba(148, 163, 184, 0.18);
          border-radius: 18px;
          padding: 0.78rem 0.88rem;
          box-shadow: 0 16px 36px rgba(15, 23, 42, 0.05);
        }
        div[data-testid="stMetricLabel"] {
          color: #475569 !important;
        }
        div[data-testid="stMetricValue"] {
          line-height: 1.02 !important;
          color: #0f172a !important;
        }
        div[data-testid="stMetricValue"] > div {
          font-size: clamp(0.98rem, 1vw, 1.42rem) !important;
          white-space: normal !important;
          overflow: visible !important;
          text-overflow: clip !important;
          word-break: keep-all !important;
          line-height: 1.08 !important;
          letter-spacing: -0.02em;
          color: #0f172a !important;
        }
        div[data-testid="stMetricDelta"] {
          color: #0f766e !important;
        }
        div.stButton > button,
        div[data-testid="stBaseButton-secondary"] > button {
          border-radius: 14px;
          font-weight: 700;
          border: 0;
          box-shadow: 0 10px 22px rgba(15, 23, 42, 0.08);
        }
        .stTabs [data-baseweb="tab-list"] {
          gap: 0.5rem;
        }
        .stTabs [data-baseweb="tab"] {
          background: rgba(255, 255, 255, 0.72);
          border: 1px solid rgba(148, 163, 184, 0.18);
          border-radius: 14px;
          padding: 0 1rem;
          height: 2.8rem;
        }
        .stTabs [aria-selected="true"] {
          background: linear-gradient(135deg, rgba(14,165,233,0.14), rgba(16,185,129,0.14));
        }
        .fv-hero {
          background: linear-gradient(135deg, rgba(15,23,42,0.95), rgba(15,118,110,0.92));
          border-radius: 24px;
          padding: 1.3rem 1.5rem 1.35rem;
          color: white;
          box-shadow: 0 24px 52px rgba(15, 23, 42, 0.18);
          margin-bottom: 1rem;
        }
        .fv-hero,
        .fv-hero * {
          color: #ffffff !important;
        }
        .fv-hero__top {
          display: flex;
          align-items: center;
          gap: 0.85rem;
          margin-bottom: 0.45rem;
        }
        .fv-hero__icon {
          width: 2.8rem;
          height: 2.8rem;
          border-radius: 16px;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          background: rgba(255,255,255,0.12);
          font-size: 1.35rem;
        }
        .fv-hero__title {
          font-size: 1.7rem;
          font-weight: 800;
          letter-spacing: -0.02em;
          margin: 0;
        }
        .fv-hero__subtitle {
          margin: 0;
          color: rgba(241, 245, 249, 0.82);
          line-height: 1.6;
          font-size: 0.96rem;
        }
        .fv-app-shell {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 1rem;
          margin-bottom: 1rem;
        }
        .fv-app-brand {
          display: flex;
          flex-direction: column;
          gap: 0.2rem;
        }
        .fv-app-title {
          font-size: 2rem;
          font-weight: 900;
          letter-spacing: -0.04em;
          color: #0f172a;
          margin: 0;
        }
        .fv-app-subtitle {
          margin: 0;
          font-size: 0.95rem;
          color: #64748b;
        }
        .fv-stat-grid {
          display: grid;
          grid-template-columns: repeat(var(--fv-stat-columns, 4), minmax(0, 1fr));
          gap: 0.8rem;
          margin: 0.9rem 0 0.65rem;
        }
        .fv-stat-card {
          background: linear-gradient(180deg, rgba(255,255,255,0.94), rgba(248,250,252,0.94));
          border: 1px solid rgba(148, 163, 184, 0.16);
          border-radius: 18px;
          padding: 0.8rem 0.88rem 0.74rem;
          min-width: 0;
          box-shadow: 0 14px 32px rgba(15, 23, 42, 0.05);
        }
        .fv-stat-label {
          font-size: 0.82rem;
          font-weight: 700;
          color: #64748b;
          margin: 0 0 0.28rem;
          letter-spacing: -0.01em;
        }
        .fv-stat-value {
          font-size: clamp(1.18rem, 1.28vw, 1.68rem);
          font-weight: 800;
          letter-spacing: -0.03em;
          line-height: 1.08;
          color: #0f172a;
          margin: 0;
          word-break: keep-all;
          white-space: nowrap;
        }
        .fv-chip-row {
          display: flex;
          gap: 0.45rem;
          flex-wrap: wrap;
          margin-top: 0.9rem;
        }
        .fv-chip {
          display: inline-flex;
          align-items: center;
          padding: 0.28rem 0.6rem;
          border-radius: 999px;
          background: rgba(255,255,255,0.12);
          color: rgba(248,250,252,0.92);
          font-size: 0.82rem;
          border: 1px solid rgba(255,255,255,0.12);
        }
        .fv-reco-row {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 0.9rem;
          padding: 0.2rem 0 0.25rem;
        }
        .fv-reco-main {
          min-width: 0;
        }
        .fv-reco-matchup {
          margin: 0;
          font-size: 1.06rem;
          font-weight: 800;
          letter-spacing: -0.02em;
          color: #0f172a;
        }
        .fv-reco-sub {
          margin: 0.28rem 0 0;
          color: #64748b;
          font-size: 0.9rem;
          line-height: 1.5;
        }
        .fv-reco-side {
          display: flex;
          align-items: center;
          justify-content: flex-end;
          gap: 0.55rem;
          flex-wrap: wrap;
        }
        .fv-reco-badge {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-height: 2rem;
          padding: 0.42rem 0.82rem;
          border-radius: 999px;
          font-size: 0.88rem;
          font-weight: 800;
          line-height: 1;
          letter-spacing: -0.01em;
          border: 1px solid transparent;
          white-space: nowrap;
        }
        .fv-reco-badge--home {
          background: rgba(14, 165, 233, 0.16);
          color: #0c4a6e;
          border-color: rgba(14, 165, 233, 0.24);
        }
        .fv-reco-badge--draw {
          background: rgba(245, 158, 11, 0.18);
          color: #9a3412;
          border-color: rgba(245, 158, 11, 0.28);
        }
        .fv-reco-badge--away {
          background: rgba(239, 68, 68, 0.14);
          color: #991b1b;
          border-color: rgba(239, 68, 68, 0.24);
        }
        .fv-reco-badge--score {
          background: rgba(15, 23, 42, 0.06);
          color: #0f172a;
          border-color: rgba(148, 163, 184, 0.22);
        }
        @media (max-width: 1180px) {
          .fv-stat-grid {
            grid-template-columns: repeat(2, minmax(0, 1fr));
          }
        }
        @media (max-width: 720px) {
          .fv-app-shell {
            flex-direction: column;
            align-items: flex-start;
          }
          .fv-stat-grid {
            grid-template-columns: 1fr;
          }
          .fv-reco-row {
            flex-direction: column;
          }
          .fv-reco-side {
            justify-content: flex-start;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_page_banner(
    *,
    title: str,
    subtitle: str,
    emoji: str,
    chips: list[str] | None = None,
) -> None:
    """渲染页面横幅。"""

    chip_html = ""
    if chips:
        chip_html = '<div class="fv-chip-row">' + "".join(
            f'<span class="fv-chip">{chip}</span>' for chip in chips
        ) + "</div>"

    st.markdown(
        f"""
        <section class="fv-hero">
          <div class="fv-hero__top">
            <div class="fv-hero__icon">{emoji}</div>
            <h2 class="fv-hero__title">{title}</h2>
          </div>
          <p class="fv-hero__subtitle">{subtitle}</p>
          {chip_html}
        </section>
        """,
        unsafe_allow_html=True,
    )
