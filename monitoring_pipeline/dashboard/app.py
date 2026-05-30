"""Streamlit business dashboard — KKBox Churn Prediction."""
import os

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

FASTAPI_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")

st.set_page_config(
    page_title="KKBox Churn Prediction",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

/* ── Music note background ── */
.stApp {
    background-color: #f8f7f5;
    background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='120' height='120'%3E%3Ctext x='10' y='50' font-size='28' fill='rgba(207,111,60,0.07)' font-family='serif'%3E%E2%99%AA%3C/text%3E%3Ctext x='65' y='95' font-size='20' fill='rgba(207,111,60,0.05)' font-family='serif'%3E%E2%99%AB%3C/text%3E%3Ctext x='80' y='30' font-size='14' fill='rgba(207,111,60,0.04)' font-family='serif'%3E%E2%99%A9%3C/text%3E%3C/svg%3E");
}

.block-container {
    padding: 2.5rem 3rem 3rem 3rem;
    max-width: 1100px;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #1c0f2e 0%, #110a1f 100%) !important;
    border-right: 1px solid rgba(255,255,255,0.06);
}
section[data-testid="stSidebar"] > div {
    padding-top: 1.5rem;
    padding-bottom: 1.5rem;
}
/* All text in sidebar */
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] div {
    color: #e2d9f3 !important;
}
section[data-testid="stSidebar"] h3 {
    color: #ffffff !important;
}
/* Sidebar input */
section[data-testid="stSidebar"] input {
    background: rgba(255,255,255,0.1) !important;
    border: 1px solid rgba(255,255,255,0.2) !important;
    color: #f0ebfa !important;
    border-radius: 8px !important;
    caret-color: #CF6F3C !important;
}
section[data-testid="stSidebar"] input::placeholder {
    color: rgba(255,255,255,0.35) !important;
}
section[data-testid="stSidebar"] hr {
    border-color: rgba(255,255,255,0.1) !important;
}

/* ── Main area inputs ── */
div[data-testid="stTextInput"] input {
    border-radius: 8px !important;
    border: 1px solid #d1d5db !important;
    font-size: 0.9rem !important;
    padding: 0.6rem 0.75rem !important;
    background: #ffffff !important;
    color: #111827 !important;
}
div[data-testid="stTextInput"] input:focus {
    border-color: #CF6F3C !important;
    box-shadow: 0 0 0 3px rgba(207,111,60,0.15) !important;
}
div[data-testid="stTextInput"] input::placeholder {
    color: #9ca3af !important;
}

/* ── Header ── */
.app-header {
    display: flex;
    align-items: center;
    gap: 16px;
    margin-bottom: 0.2rem;
}
.app-header h1 {
    font-size: 1.65rem;
    font-weight: 700;
    color: #1a1a1a;
    margin: 0;
    letter-spacing: -0.4px;
}
.waveform {
    display: flex;
    align-items: center;
    gap: 3px;
    height: 36px;
}
.waveform span {
    display: inline-block;
    width: 4px;
    border-radius: 2px;
    background: #CF6F3C;
    opacity: 0.85;
}
.app-subtitle {
    color: #6b7280;
    font-size: 0.9rem;
    margin-bottom: 2rem;
    margin-left: 2px;
}

/* ── Card title ── */
.card-title {
    font-size: 0.75rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: #9ca3af;
    margin-bottom: 1rem;
}

/* ── Badges ── */
.badge-churn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: #fef2f2;
    color: #dc2626;
    border: 1px solid #fecaca;
    border-radius: 999px;
    padding: 5px 16px;
    font-size: 0.85rem;
    font-weight: 600;
}
.badge-retain {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: #f0fdf4;
    color: #16a34a;
    border: 1px solid #bbf7d0;
    border-radius: 999px;
    padding: 5px 16px;
    font-size: 0.85rem;
    font-weight: 600;
}

/* ── Stat boxes ── */
.stat-box {
    background: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 12px;
    padding: 1.1rem 1.25rem;
    text-align: center;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
}
.stat-value {
    font-size: 1.9rem;
    font-weight: 700;
    color: #111827;
    line-height: 1.2;
}
.stat-label {
    font-size: 0.72rem;
    color: #9ca3af;
    margin-top: 3px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.07em;
}
.stat-value.danger  { color: #dc2626; }
.stat-value.success { color: #16a34a; }
.stat-value.neutral { color: #CF6F3C; }

/* ── Buttons ── */
div[data-testid="stFormSubmitButton"] button,
button[kind="primary"] {
    background: #CF6F3C !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 0.9rem !important;
    padding: 0.55rem 1.5rem !important;
    color: white !important;
    transition: background 0.2s ease !important;
}
div[data-testid="stFormSubmitButton"] button:hover,
button[kind="primary"]:hover {
    background: #b85e2f !important;
}

/* ── Tabs ── */
div[data-testid="stTabs"] button {
    font-size: 0.9rem;
    font-weight: 500;
    color: #6b7280;
}
div[data-testid="stTabs"] button[aria-selected="true"] {
    color: #CF6F3C;
    border-bottom-color: #CF6F3C !important;
}

/* ── Misc ── */
hr { border-color: #f3f4f6; }
div[data-testid="stForm"] { border: none !important; padding: 0 !important; background: transparent !important; }
</style>
""", unsafe_allow_html=True)


def api_healthy(url: str) -> bool:
    try:
        return requests.get(f"{url}/health", timeout=5).ok
    except Exception:
        return False


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:1.25rem;">
        <span style="font-size:1.4rem">🎵</span>
        <span style="font-size:0.95rem;font-weight:600;letter-spacing:-0.2px;">KKBox Churn</span>
    </div>
    """, unsafe_allow_html=True)
    api_url = st.text_input("API URL", value=FASTAPI_URL)
    st.divider()
    healthy = api_healthy(api_url)
    if healthy:
        st.success("API is online")
    else:
        st.error("API is offline")
    st.markdown("<br><br>", unsafe_allow_html=True)
    st.markdown(
        '<p style="font-size:0.72rem;color:rgba(255,255,255,0.25);margin:0;">v1.0 · KKBox Churn Prediction</p>',
        unsafe_allow_html=True,
    )

# ── Header ────────────────────────────────────────────────────────────────────

st.markdown("""
<div class="app-header">
    <div class="waveform" aria-hidden="true">
        <span style="height:10px"></span>
        <span style="height:22px"></span>
        <span style="height:32px"></span>
        <span style="height:18px"></span>
        <span style="height:28px"></span>
        <span style="height:14px"></span>
        <span style="height:36px"></span>
        <span style="height:24px"></span>
        <span style="height:16px"></span>
        <span style="height:30px"></span>
        <span style="height:10px"></span>
    </div>
    <h1>KKBox Churn Prediction</h1>
</div>
<p class="app-subtitle">Predict subscriber churn probability using real-time feature lookup from the online feature store.</p>
""", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["Single User", "Batch Prediction", "Statistics"])

# ── Tab 1: Single ─────────────────────────────────────────────────────────────

with tab1:
    left, right = st.columns([1, 1], gap="large")

    with left:
        st.markdown('<p class="card-title">Member Lookup</p>', unsafe_allow_html=True)

        if st.button("🎲 Random sample", key="random_btn"):
            try:
                r = requests.get(f"{api_url}/sample?n=1", timeout=5)
                r.raise_for_status()
                msnos = r.json().get("msnos", [])
                if msnos:
                    st.session_state["msno_input"] = msnos[0]
            except Exception:
                st.warning("Could not fetch sample from API.")

        with st.form("single_form"):
            msno = st.text_input(
                "Member ID (msno)",
                placeholder="e.g. 9bq7LhZP3z8FutAux8UkG13mSW...",
                value=st.session_state.get("msno_input", ""),
            )
            predict_btn = st.form_submit_button("Predict churn risk", use_container_width=True, type="primary")

        st.markdown("""
        <p style="font-size:0.78rem;color:#9ca3af;margin-top:0.75rem;line-height:1.5;">
        Enter the hashed member ID (msno) to look up features from the online store
        and get a real-time churn prediction.
        </p>
        """, unsafe_allow_html=True)

    with right:
        if predict_btn and msno:
            with st.spinner(""):
                try:
                    resp = requests.post(f"{api_url}/predict/", json={"msno": msno}, timeout=10)
                    resp.raise_for_status()
                    data = resp.json()
                    prob = data["churn_probability"]
                    is_churn = data["is_churn"]
                    member_found = data.get("member_found", True)

                    if not member_found:
                        st.warning(
                            "Member not found in feature store. "
                            "Prediction uses population-average features and may not be reliable.",
                            icon="⚠️",
                        )

                    st.markdown('<p class="card-title">Prediction Result</p>', unsafe_allow_html=True)

                    color = "#dc2626" if is_churn else "#16a34a"
                    fig = go.Figure(go.Indicator(
                        mode="gauge+number",
                        value=round(prob * 100, 1),
                        number={"suffix": "%", "font": {"size": 42, "color": color}},
                        gauge={
                            "axis": {"range": [0, 100], "ticksuffix": "%", "tickfont": {"size": 11}, "tickcolor": "#9ca3af"},
                            "bar": {"color": color, "thickness": 0.26},
                            "bgcolor": "#f4f3f1",
                            "borderwidth": 0,
                            "steps": [
                                {"range": [0, 78.1], "color": "#ecfdf5"},
                                {"range": [78.1, 100], "color": "#fff1f2"},
                            ],
                            "threshold": {
                                "line": {"color": "#6b7280", "width": 2},
                                "thickness": 0.8,
                                "value": 78.1,
                            },
                        },
                    ))
                    fig.update_layout(
                        height=230,
                        margin=dict(t=30, b=0, l=30, r=30),
                        paper_bgcolor="rgba(0,0,0,0)",
                        font={"family": "Inter", "color": "#374151"},
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    badge = (
                        '<span class="badge-churn">⚠ High churn risk</span>'
                        if is_churn else
                        '<span class="badge-retain">✓ Low churn risk</span>'
                    )
                    st.markdown(badge, unsafe_allow_html=True)
                    st.markdown(f"""
                    <p style="font-size:0.82rem;color:#6b7280;margin-top:0.5rem;">
                    Threshold: 78.1%&nbsp;·&nbsp;Score: <b>{prob:.1%}</b>
                    </p>
                    """, unsafe_allow_html=True)

                    reasons = data.get("reasons", [])
                    if reasons:
                        if is_churn:
                            header = "🔴 Lý do dự đoán churn"
                            item_color = "#dc2626"
                            bg = "#fef2f2"
                            border = "#fecaca"
                        else:
                            header = "⚠️ Cảnh báo tiềm ẩn"
                            item_color = "#b45309"
                            bg = "#fffbeb"
                            border = "#fde68a"

                        items_html = "".join(
                            f'<li style="margin-bottom:6px;color:{item_color};">{r}</li>'
                            for r in reasons
                        )
                        st.markdown(f"""
                        <div style="background:{bg};border:1px solid {border};
                                    border-radius:10px;padding:0.9rem 1rem;margin-top:0.75rem;">
                            <p style="font-size:0.78rem;font-weight:700;color:{item_color};
                                      text-transform:uppercase;letter-spacing:0.07em;margin-bottom:0.5rem;">
                                {header}
                            </p>
                            <ul style="margin:0;padding-left:1.2rem;font-size:0.85rem;line-height:1.6;">
                                {items_html}
                            </ul>
                        </div>
                        """, unsafe_allow_html=True)

                except requests.exceptions.ConnectionError:
                    st.error("Cannot reach the API. Check the URL in the sidebar.")
                except Exception as e:
                    st.error(f"Error: {e}")

        elif predict_btn:
            st.warning("Please enter a member ID.")
        else:
            st.markdown("""
            <div style="height:220px;display:flex;flex-direction:column;
                        align-items:center;justify-content:center;
                        background:#ffffff;border-radius:14px;border:1px dashed #d1d5db;">
                <svg width="48" height="36" viewBox="0 0 48 36" fill="none" xmlns="http://www.w3.org/2000/svg" style="opacity:0.2">
                    <rect x="0"  y="12" width="5" height="12" rx="2.5" fill="#CF6F3C"/>
                    <rect x="7"  y="6"  width="5" height="24" rx="2.5" fill="#CF6F3C"/>
                    <rect x="14" y="0"  width="5" height="36" rx="2.5" fill="#CF6F3C"/>
                    <rect x="21" y="8"  width="5" height="20" rx="2.5" fill="#CF6F3C"/>
                    <rect x="28" y="2"  width="5" height="32" rx="2.5" fill="#CF6F3C"/>
                    <rect x="35" y="9"  width="5" height="18" rx="2.5" fill="#CF6F3C"/>
                    <rect x="42" y="14" width="5" height="8"  rx="2.5" fill="#CF6F3C"/>
                </svg>
                <p style="color:#9ca3af;font-size:0.85rem;margin-top:1rem;">Result will appear here</p>
            </div>
            """, unsafe_allow_html=True)

# ── Tab 2: Batch ──────────────────────────────────────────────────────────────

with tab2:
    st.markdown('<p class="card-title">Batch Prediction</p>', unsafe_allow_html=True)
    st.markdown(
        '<p style="font-size:0.85rem;color:#6b7280;margin-bottom:1rem;">'
        'Upload a CSV with a column named <code>msno</code>. '
        'Results include churn probability and risk label for each member.</p>',
        unsafe_allow_html=True,
    )

    uploaded = st.file_uploader("", type=["csv"], label_visibility="collapsed")

    if uploaded:
        df = pd.read_csv(uploaded)
        if "msno" not in df.columns:
            st.error("CSV must contain a column named `msno`.")
        else:
            msno_list = df["msno"].dropna().tolist()
            st.info(f"{len(msno_list)} records loaded")

            if st.button("▶  Run Batch Prediction", type="primary"):
                with st.spinner(f"Running predictions for {len(msno_list)} users..."):
                    try:
                        resp = requests.post(
                            f"{api_url}/predict/batch",
                            json={"msno_list": msno_list},
                            timeout=120,
                        )
                        resp.raise_for_status()
                        results = resp.json()
                        result_df = pd.DataFrame(results)

                        churn_n    = int(result_df["is_churn"].sum())
                        retain_n   = len(result_df) - churn_n
                        churn_rate = churn_n / len(result_df)

                        st.divider()
                        c1, c2, c3, c4 = st.columns(4)
                        with c1:
                            st.markdown(f'<div class="stat-box"><div class="stat-value">{len(result_df)}</div><div class="stat-label">Total Users</div></div>', unsafe_allow_html=True)
                        with c2:
                            st.markdown(f'<div class="stat-box"><div class="stat-value danger">{churn_n}</div><div class="stat-label">Predicted Churn</div></div>', unsafe_allow_html=True)
                        with c3:
                            st.markdown(f'<div class="stat-box"><div class="stat-value success">{retain_n}</div><div class="stat-label">Predicted Retain</div></div>', unsafe_allow_html=True)
                        with c4:
                            cls = "danger" if churn_rate > 0.3 else "success"
                            st.markdown(f'<div class="stat-box"><div class="stat-value {cls}">{churn_rate:.1%}</div><div class="stat-label">Churn Rate</div></div>', unsafe_allow_html=True)

                        st.markdown("<br>", unsafe_allow_html=True)
                        col_chart, col_table = st.columns([1.1, 1], gap="large")

                        with col_chart:
                            fig = go.Figure()
                            fig.add_trace(go.Histogram(
                                x=result_df["churn_probability"],
                                nbinsx=25,
                                marker_color="#93c5fd",
                                marker_line_color="white",
                                marker_line_width=1,
                            ))
                            fig.add_vline(
                                x=0.781, line_dash="dot", line_color="#CF6F3C", line_width=2,
                                annotation_text="threshold", annotation_font_size=11,
                                annotation_font_color="#CF6F3C",
                            )
                            fig.update_layout(
                                title=dict(text="Score Distribution", font=dict(size=13, family="Inter")),
                                xaxis=dict(title="Churn Probability", tickformat=".0%", range=[0, 1]),
                                yaxis=dict(title="Users"),
                                paper_bgcolor="rgba(0,0,0,0)",
                                plot_bgcolor="#fafaf9",
                                font=dict(family="Inter", color="#374151", size=11),
                                height=300,
                                margin=dict(t=40, b=40, l=10, r=10),
                                showlegend=False,
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        with col_table:
                            st.markdown('<p style="font-size:0.82rem;font-weight:600;color:#374151;margin-bottom:0.5rem;">Top churn risks</p>', unsafe_allow_html=True)
                            top = (
                                result_df[result_df["is_churn"] == 1]
                                .sort_values("churn_probability", ascending=False)
                                .head(8)
                                .copy()
                            )
                            if len(top):
                                top["Score"] = top["churn_probability"].map(lambda x: f"{x:.1%}")
                                top["Risk"]  = "⚠ Churn"
                                st.dataframe(
                                    top[["msno", "Score", "Risk"]].rename(columns={"msno": "Member ID"}),
                                    use_container_width=True,
                                    hide_index=True,
                                    height=260,
                                )
                            else:
                                st.success("No high-risk users in this batch.")

                        csv = result_df.to_csv(index=False).encode("utf-8")
                        st.download_button(
                            "⬇ Download full results (CSV)",
                            csv, "churn_predictions.csv", "text/csv",
                            use_container_width=True,
                        )

                    except requests.exceptions.ConnectionError:
                        st.error("Cannot reach the API. Check the URL in the sidebar.")
                    except Exception as e:
                        st.error(f"Error: {e}")

# ── Tab 3: Statistics ─────────────────────────────────────────────────────────

with tab3:
    st.markdown('<p class="card-title">Prediction Statistics</p>', unsafe_allow_html=True)
    st.markdown(
        '<p style="font-size:0.85rem;color:#6b7280;margin-bottom:1.5rem;">'
        'Cumulative counts since the API last started. Click Refresh to update.</p>',
        unsafe_allow_html=True,
    )

    col_refresh, _ = st.columns([1, 5])
    with col_refresh:
        st.button("↻ Refresh", key="refresh_stats")

    try:
        r = requests.get(f"{api_url}/stats", timeout=5)
        r.raise_for_status()
        s = r.json()
        total      = s["total_predictions"]
        churn_n    = s["churn_count"]
        retain_n   = s["retain_count"]
        churn_rate = s["churn_rate"]

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f'<div class="stat-box"><div class="stat-value">{total:,}</div><div class="stat-label">Total Predictions</div></div>', unsafe_allow_html=True)
        with c2:
            st.markdown(f'<div class="stat-box"><div class="stat-value danger">{churn_n:,}</div><div class="stat-label">Churn</div></div>', unsafe_allow_html=True)
        with c3:
            st.markdown(f'<div class="stat-box"><div class="stat-value success">{retain_n:,}</div><div class="stat-label">Retain</div></div>', unsafe_allow_html=True)
        with c4:
            cls = "danger" if churn_rate > 0.3 else "success"
            st.markdown(f'<div class="stat-box"><div class="stat-value {cls}">{churn_rate:.1%}</div><div class="stat-label">Churn Rate</div></div>', unsafe_allow_html=True)

        if total > 0:
            st.markdown("<br>", unsafe_allow_html=True)

            # ── Churn user list ──
            try:
                cr = requests.get(f"{api_url}/stats/churned?limit=500", timeout=5)
                cr.raise_for_status()
                churned = cr.json().get("churned", [])
                if churned:
                    st.markdown('<p class="card-title">All Predicted Churn Users</p>', unsafe_allow_html=True)
                    churn_df = pd.DataFrame(churned)
                    churn_df["Score"] = churn_df["churn_probability"].map(lambda x: f"{x:.1%}")
                    churn_df = churn_df.rename(columns={
                        "msno": "Member ID",
                        "churn_probability": "Probability",
                        "predicted_at": "Predicted At",
                    })

                    search = st.text_input("🔍 Filter by Member ID", placeholder="Type to filter...", key="churn_search")
                    if search:
                        churn_df = churn_df[churn_df["Member ID"].str.contains(search, case=False, na=False)]

                    st.markdown(f'<p style="font-size:0.8rem;color:#9ca3af;margin-bottom:0.5rem;">{len(churn_df)} users predicted to churn</p>', unsafe_allow_html=True)
                    display_cols = [c for c in ["Member ID", "Score", "Predicted At"] if c in churn_df.columns]
                    st.dataframe(
                        churn_df[display_cols],
                        use_container_width=True,
                        hide_index=True,
                        height=min(400, 40 + len(churn_df) * 35),
                    )
                    csv = pd.DataFrame(churned).to_csv(index=False).encode("utf-8")
                    st.download_button(
                        "⬇ Download churn list (CSV)",
                        csv, "churn_users.csv", "text/csv",
                        use_container_width=False,
                    )
            except Exception:
                pass

            st.markdown("<br>", unsafe_allow_html=True)
            col_pie, col_info = st.columns([1, 1.4], gap="large")

            with col_pie:
                fig = go.Figure(go.Pie(
                    labels=["Churn", "Retain"],
                    values=[churn_n, retain_n],
                    marker_colors=["#fca5a5", "#86efac"],
                    hole=0.6,
                    textinfo="percent",
                    textfont=dict(size=13, family="Inter"),
                    hovertemplate="%{label}: %{value:,}<extra></extra>",
                ))
                fig.add_annotation(
                    text=f"<b>{churn_rate:.1%}</b><br><span style='font-size:11px'>churn rate</span>",
                    x=0.5, y=0.5, showarrow=False,
                    font=dict(size=16, family="Inter", color="#374151"),
                )
                fig.update_layout(
                    height=280,
                    margin=dict(t=20, b=20, l=0, r=0),
                    paper_bgcolor="rgba(0,0,0,0)",
                    showlegend=True,
                    legend=dict(font=dict(family="Inter", size=12), orientation="h", y=-0.08),
                )
                st.plotly_chart(fig, use_container_width=True)

            with col_info:
                st.markdown("<br>", unsafe_allow_html=True)
                rows = [
                    ("Total predictions made", f"{total:,}"),
                    ("Predicted to churn", f"{churn_n:,} ({churn_rate:.1%})"),
                    ("Predicted to retain", f"{retain_n:,} ({1-churn_rate:.1%})"),
                    ("Decision threshold", "78.1%"),
                    ("Model", "XGBoost · AUC-ROC 0.8926"),
                ]
                table_html = '<table style="width:100%;border-collapse:collapse;font-size:0.875rem;">'
                for label, value in rows:
                    table_html += f"""
                    <tr style="border-bottom:1px solid #f3f4f6;">
                        <td style="padding:0.65rem 0.5rem;color:#6b7280;font-weight:500;">{label}</td>
                        <td style="padding:0.65rem 0.5rem;color:#111827;font-weight:600;text-align:right;">{value}</td>
                    </tr>"""
                table_html += "</table>"
                st.markdown(
                    f'<div style="background:#ffffff;border:1px solid #e5e7eb;border-radius:12px;padding:0.5rem 1rem;">{table_html}</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.info("No predictions made yet. Run some predictions first.")

    except requests.exceptions.ConnectionError:
        st.error("Cannot reach the API. Check the URL in the sidebar.")
    except Exception as e:
        st.error(f"Error loading stats: {e}")
