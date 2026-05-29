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

.block-container {
    padding: 2.5rem 3rem 3rem 3rem;
    max-width: 1100px;
}

/* Header */
.app-header {
    display: flex;
    align-items: center;
    gap: 14px;
    margin-bottom: 0.25rem;
}
.app-header h1 {
    font-size: 1.6rem;
    font-weight: 600;
    color: #1a1a1a;
    margin: 0;
    letter-spacing: -0.3px;
}
.app-subtitle {
    color: #6b7280;
    font-size: 0.9rem;
    margin-bottom: 2rem;
}

/* Cards */
.card {
    background: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 14px;
    padding: 1.75rem;
    margin-bottom: 1.25rem;
}
.card-title {
    font-size: 0.8rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #9ca3af;
    margin-bottom: 1rem;
}

/* Result badge */
.badge-churn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: #fef2f2;
    color: #dc2626;
    border: 1px solid #fecaca;
    border-radius: 999px;
    padding: 5px 14px;
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
    padding: 5px 14px;
    font-size: 0.85rem;
    font-weight: 600;
}

/* Stat box */
.stat-box {
    background: #f9fafb;
    border: 1px solid #e5e7eb;
    border-radius: 10px;
    padding: 1rem 1.25rem;
    text-align: center;
}
.stat-value {
    font-size: 1.8rem;
    font-weight: 700;
    color: #111827;
    line-height: 1.2;
}
.stat-label {
    font-size: 0.78rem;
    color: #6b7280;
    margin-top: 2px;
}
.stat-value.danger { color: #dc2626; }
.stat-value.success { color: #16a34a; }

/* Input styling */
div[data-testid="stTextInput"] input {
    border-radius: 8px;
    border: 1px solid #d1d5db;
    font-size: 0.9rem;
    padding: 0.6rem 0.75rem;
}
div[data-testid="stTextInput"] input:focus {
    border-color: #CF6F3C;
    box-shadow: 0 0 0 3px rgba(207,111,60,0.15);
}

/* Button */
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
div[data-testid="stFormSubmitButton"] button:hover {
    background: #b85e2f !important;
}

/* Tabs */
div[data-testid="stTabs"] button {
    font-size: 0.9rem;
    font-weight: 500;
    color: #6b7280;
    border-radius: 8px 8px 0 0;
}
div[data-testid="stTabs"] button[aria-selected="true"] {
    color: #CF6F3C;
    border-bottom-color: #CF6F3C !important;
}

/* Divider */
hr { border-color: #f3f4f6; }

/* Hide form border */
div[data-testid="stForm"] { border: none !important; padding: 0 !important; background: transparent !important; }

/* Sidebar */
section[data-testid="stSidebar"] { background: #f9fafb; }
</style>
""", unsafe_allow_html=True)


def api_healthy(url: str) -> bool:
    try:
        return requests.get(f"{url}/health", timeout=5).ok
    except Exception:
        return False


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### Settings")
    api_url = st.text_input("API URL", value=FASTAPI_URL, label_visibility="visible")
    st.divider()
    if api_healthy(api_url):
        st.success("● API is online", icon=None)
    else:
        st.error("● API is offline")

# ── Header ────────────────────────────────────────────────────────────────────

st.markdown("""
<div class="app-header">
    <span style="font-size:2rem">🎵</span>
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
        with st.form("single_form"):
            msno = st.text_input(
                "Member ID (msno)",
                placeholder="e.g. 9bq7LhZP3z8FutAux8UkG13mSW...",
            )
            predict_btn = st.form_submit_button("Predict churn risk", use_container_width=True, type="primary")

        st.markdown("""
        <p style="font-size:0.78rem; color:#9ca3af; margin-top:0.75rem;">
        Enter the hashed member ID (msno) to look up features from the online store and get a real-time churn prediction.
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

                    fig = go.Figure(go.Indicator(
                        mode="gauge+number",
                        value=round(prob * 100, 1),
                        number={"suffix": "%", "font": {"size": 40, "color": "#dc2626" if is_churn else "#16a34a"}},
                        gauge={
                            "axis": {"range": [0, 100], "ticksuffix": "%", "tickfont": {"size": 11}, "tickcolor": "#9ca3af"},
                            "bar": {"color": "#dc2626" if is_churn else "#16a34a", "thickness": 0.25},
                            "bgcolor": "#f9fafb",
                            "borderwidth": 0,
                            "steps": [
                                {"range": [0, 78.1], "color": "#f0fdf4"},
                                {"range": [78.1, 100], "color": "#fef2f2"},
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
                    <p style="font-size:0.82rem; color:#6b7280; margin-top:0.5rem;">
                    Threshold: 78.1% &nbsp;·&nbsp; Score: <b>{prob:.1%}</b>
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
            <div style="height:220px; display:flex; flex-direction:column;
                        align-items:center; justify-content:center;
                        background:#f9fafb; border-radius:12px; border:1px dashed #d1d5db;">
                <span style="font-size:2rem; opacity:0.3">📊</span>
                <p style="color:#9ca3af; font-size:0.85rem; margin-top:0.5rem;">
                    Result will appear here
                </p>
            </div>
            """, unsafe_allow_html=True)

# ── Tab 2: Batch ──────────────────────────────────────────────────────────────

with tab2:
    st.markdown('<p class="card-title">Batch Prediction</p>', unsafe_allow_html=True)
    st.markdown('<p style="font-size:0.85rem; color:#6b7280; margin-bottom:1rem;">Upload a CSV file with a column named <code>msno</code>. Results include churn probability and risk label for each user.</p>', unsafe_allow_html=True)

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
                        results = resp.json()["predictions"]
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
                            st.markdown(f'<div class="stat-box"><div class="stat-value {"danger" if churn_rate > 0.3 else "success"}">{churn_rate:.1%}</div><div class="stat-label">Churn Rate</div></div>', unsafe_allow_html=True)

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
                                name="Distribution",
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
                                plot_bgcolor="#f9fafb",
                                font=dict(family="Inter", color="#374151", size=11),
                                height=300,
                                margin=dict(t=40, b=40, l=10, r=10),
                                showlegend=False,
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        with col_table:
                            st.markdown('<p style="font-size:0.82rem; font-weight:600; color:#374151; margin-bottom:0.5rem;">Top churn risks</p>', unsafe_allow_html=True)
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
