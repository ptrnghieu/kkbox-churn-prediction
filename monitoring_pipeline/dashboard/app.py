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

/* ── Music note background pattern ── */
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
section[data-testid="stSidebar"] .stMarkdown,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] h3 {
    color: #e2d9f3 !important;
}
section[data-testid="stSidebar"] div[data-testid="stTextInput"] input {
    background: rgba(255,255,255,0.08) !important;
    border: 1px solid rgba(255,255,255,0.15) !important;
    color: #f0ebfa !important;
    border-radius: 8px;
}
section[data-testid="stSidebar"] hr {
    border-color: rgba(255,255,255,0.1) !important;
}
/* Sidebar status text override */
section[data-testid="stSidebar"] .stAlert p {
    color: inherit !important;
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

/* ── Cards ── */
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
    padding: 1rem 1.25rem;
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
    font-size: 0.75rem;
    color: #9ca3af;
    margin-top: 3px;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.06em;
}
.stat-value.danger { color: #dc2626; }
.stat-value.success { color: #16a34a; }

/* ── Inputs ── */
div[data-testid="stTextInput"] input {
    border-radius: 8px;
    border: 1px solid #d1d5db;
    font-size: 0.9rem;
    padding: 0.6rem 0.75rem;
    background: #ffffff;
}
div[data-testid="stTextInput"] input:focus {
    border-color: #CF6F3C;
    box-shadow: 0 0 0 3px rgba(207,111,60,0.15);
}

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
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:1.5rem;padding-top:0.5rem;">
        <span style="font-size:1.5rem">🎵</span>
        <span style="font-size:1rem;font-weight:600;color:#e2d9f3;letter-spacing:-0.2px;">KKBox</span>
    </div>
    """, unsafe_allow_html=True)
    api_url = st.text_input("API URL", value=FASTAPI_URL)
    st.divider()
    healthy = api_healthy(api_url)
    if healthy:
        st.success("API is online")
    else:
        st.error("API is offline")
    st.markdown("""
    <div style="position:absolute;bottom:2rem;left:1.5rem;right:1.5rem;">
        <p style="font-size:0.72rem;color:rgba(255,255,255,0.3);margin:0;">KKBox Churn · v1.0</p>
    </div>
    """, unsafe_allow_html=True)

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

tab1, tab2 = st.tabs(["Single User", "Batch Prediction"])

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
                <svg width="40" height="32" viewBox="0 0 40 32" fill="none" xmlns="http://www.w3.org/2000/svg" style="opacity:0.2">
                    <rect x="0"  y="10" width="4" height="12" rx="2" fill="#CF6F3C"/>
                    <rect x="6"  y="4"  width="4" height="24" rx="2" fill="#CF6F3C"/>
                    <rect x="12" y="0"  width="4" height="32" rx="2" fill="#CF6F3C"/>
                    <rect x="18" y="6"  width="4" height="20" rx="2" fill="#CF6F3C"/>
                    <rect x="24" y="2"  width="4" height="28" rx="2" fill="#CF6F3C"/>
                    <rect x="30" y="8"  width="4" height="16" rx="2" fill="#CF6F3C"/>
                    <rect x="36" y="12" width="4" height="8"  rx="2" fill="#CF6F3C"/>
                </svg>
                <p style="color:#9ca3af;font-size:0.85rem;margin-top:1rem;">
                    Result will appear here
                </p>
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
                        results = resp.json()  # API returns list directly
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
