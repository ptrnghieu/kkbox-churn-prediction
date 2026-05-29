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
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    .metric-card {
        background: #1e1e2e;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        border: 1px solid #313244;
    }
    .churn-high { color: #f38ba8; font-weight: 700; font-size: 1.1rem; }
    .churn-low  { color: #a6e3a1; font-weight: 700; font-size: 1.1rem; }
    .section-title { font-size: 1.3rem; font-weight: 600; margin-bottom: 0.5rem; }
    div[data-testid="stForm"] { border: none; padding: 0; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("⚙️ Settings")
    api_url = st.text_input("API URL", value=FASTAPI_URL)
    st.divider()
    st.header("🔌 API Health")
    try:
        resp = requests.get(f"{api_url}/health", timeout=5)
        if resp.ok:
            st.success("API is healthy")
        else:
            st.warning(f"Status: {resp.status_code}")
    except Exception:
        st.error("API unreachable")

# ── Header ────────────────────────────────────────────────────────────────────

st.title("🎵 KKBox Churn Prediction")
st.caption("Predict subscriber churn probability using real-time feature lookup")
st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2 = st.tabs(["👤 Single User", "📦 Batch Prediction"])

# ── Tab 1: Single prediction ──────────────────────────────────────────────────

with tab1:
    col_input, col_result = st.columns([1, 1], gap="large")

    with col_input:
        st.markdown('<p class="section-title">Member ID</p>', unsafe_allow_html=True)
        with st.form("predict_form"):
            msno = st.text_input(
                "msno",
                placeholder="Paste member ID here...",
                label_visibility="collapsed",
            )
            submitted = st.form_submit_button("🔍 Predict", use_container_width=True, type="primary")

    with col_result:
        if submitted and msno:
            try:
                resp = requests.post(
                    f"{api_url}/predict/",
                    json={"msno": msno},
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
                prob = data["churn_probability"]
                is_churn = data["is_churn"]

                fig = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=round(prob * 100, 1),
                    number={"suffix": "%", "font": {"size": 36}},
                    gauge={
                        "axis": {"range": [0, 100], "ticksuffix": "%"},
                        "bar": {"color": "#f38ba8" if is_churn else "#a6e3a1"},
                        "steps": [
                            {"range": [0, 50],  "color": "#1e1e2e"},
                            {"range": [50, 78], "color": "#2a2a3e"},
                            {"range": [78, 100],"color": "#3a1e2e"},
                        ],
                        "threshold": {
                            "line": {"color": "#cdd6f4", "width": 2},
                            "thickness": 0.75,
                            "value": 78.1,
                        },
                    },
                    title={"text": "Churn Probability", "font": {"size": 16}},
                ))
                fig.update_layout(
                    height=260,
                    margin=dict(t=60, b=0, l=20, r=20),
                    paper_bgcolor="rgba(0,0,0,0)",
                    font_color="#cdd6f4",
                )
                st.plotly_chart(fig, use_container_width=True)

                if is_churn:
                    st.error("⚠️ High churn risk — recommend retention action")
                else:
                    st.success("✅ Low churn risk — member likely to stay")

            except requests.exceptions.ConnectionError:
                st.error(f"Cannot reach API at {api_url}")
            except Exception as e:
                st.error(f"Error: {e}")
        elif submitted:
            st.warning("Please enter a member ID")
        else:
            st.markdown("""
            <div style="height:200px; display:flex; align-items:center; justify-content:center; color:#6c7086;">
                Enter a member ID and click Predict
            </div>
            """, unsafe_allow_html=True)

# ── Tab 2: Batch prediction ───────────────────────────────────────────────────

with tab2:
    st.markdown('<p class="section-title">Upload a CSV with a column named <code>msno</code></p>', unsafe_allow_html=True)

    uploaded = st.file_uploader("Upload CSV", type=["csv"], label_visibility="collapsed")

    if uploaded:
        df = pd.read_csv(uploaded)
        if "msno" not in df.columns:
            st.error("CSV must have a column named `msno`")
        else:
            msno_list = df["msno"].dropna().tolist()

            col1, col2, col3 = st.columns(3)
            col1.metric("Records loaded", len(msno_list))

            if st.button("▶️ Run Batch Prediction", type="primary"):
                with st.spinner(f"Predicting {len(msno_list)} users..."):
                    try:
                        resp = requests.post(
                            f"{api_url}/predict/batch",
                            json={"msno_list": msno_list},
                            timeout=120,
                        )
                        resp.raise_for_status()
                        results = resp.json()["predictions"]
                        result_df = pd.DataFrame(results)

                        churn_count = result_df["is_churn"].sum()
                        churn_rate  = churn_count / len(result_df)

                        st.divider()
                        m1, m2, m3 = st.columns(3)
                        m1.metric("Total Users", len(result_df))
                        m2.metric("Predicted Churn", int(churn_count), delta=f"{churn_rate:.1%}", delta_color="inverse")
                        m3.metric("Retention Rate", f"{1 - churn_rate:.1%}")

                        col_chart, col_table = st.columns([1, 1], gap="large")

                        with col_chart:
                            fig = go.Figure(go.Histogram(
                                x=result_df["churn_probability"],
                                nbinsx=20,
                                marker_color="#89b4fa",
                                marker_line_color="#1e1e2e",
                                marker_line_width=1,
                            ))
                            fig.add_vline(x=0.781, line_dash="dash", line_color="#f38ba8", annotation_text="threshold (0.781)")
                            fig.update_layout(
                                title="Churn Probability Distribution",
                                xaxis_title="Churn Probability",
                                yaxis_title="Count",
                                paper_bgcolor="rgba(0,0,0,0)",
                                plot_bgcolor="rgba(0,0,0,0)",
                                font_color="#cdd6f4",
                                height=320,
                                margin=dict(t=40, b=40, l=40, r=20),
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        with col_table:
                            st.caption("Top churn risks")
                            top = result_df[result_df["is_churn"] == 1].sort_values("churn_probability", ascending=False).head(10)
                            top["churn_probability"] = top["churn_probability"].map(lambda x: f"{x:.1%}")
                            top["is_churn"] = top["is_churn"].map(lambda x: "⚠️ Churn" if x else "✅ Retain")
                            st.dataframe(top, use_container_width=True, hide_index=True)

                        csv = result_df.to_csv(index=False).encode("utf-8")
                        st.download_button("⬇️ Download Results CSV", csv, "churn_predictions.csv", "text/csv")

                    except requests.exceptions.ConnectionError:
                        st.error(f"Cannot reach API at {api_url}")
                    except Exception as e:
                        st.error(f"Error: {e}")
