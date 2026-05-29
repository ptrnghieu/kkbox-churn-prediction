"""Streamlit business dashboard — KKBox Churn Prediction."""
import os

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

FASTAPI_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")

st.set_page_config(page_title="KKBox Churn Prediction", layout="wide")
st.title("KKBox Churn Prediction — Business Dashboard")

# ── Single prediction ─────────────────────────────────────────────────────────

st.header("Single User Prediction")

with st.form("predict_form"):
    msno = st.text_input("Member ID (msno)", placeholder="Paste msno here")
    submitted = st.form_submit_button("Predict")

if submitted and msno:
    try:
        resp = requests.post(
            f"{FASTAPI_URL}/predict/single",
            json={"msno": msno},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        prob = data["churn_probability"]
        label = "CHURN" if data["is_churn"] else "RETAIN"
        color = "red" if data["is_churn"] else "green"

        col1, col2 = st.columns(2)
        col1.metric("Churn Probability", f"{prob:.1%}")
        col2.markdown(f"### Prediction: :{color}[{label}]")
    except requests.exceptions.ConnectionError:
        st.error(f"Cannot reach FastAPI at {FASTAPI_URL}. Is the API running?")
    except Exception as e:
        st.error(f"Error: {e}")

# ── Batch prediction ──────────────────────────────────────────────────────────

st.divider()
st.header("Batch Prediction")
st.caption("Upload a CSV with a column named `msno`")

uploaded = st.file_uploader("Upload CSV", type=["csv"])

if uploaded:
    df = pd.read_csv(uploaded)
    if "msno" not in df.columns:
        st.error("CSV must have a column named `msno`")
    else:
        msno_list = df["msno"].dropna().tolist()
        st.info(f"Loaded {len(msno_list)} records")

        if st.button("Run Batch Prediction"):
            with st.spinner("Calling API..."):
                try:
                    resp = requests.post(
                        f"{FASTAPI_URL}/predict/batch",
                        json={"msno_list": msno_list},
                        timeout=60,
                    )
                    resp.raise_for_status()
                    results = resp.json()["predictions"]
                    result_df = pd.DataFrame(results)

                    churn_count = result_df["is_churn"].sum()
                    total = len(result_df)
                    churn_rate = churn_count / total

                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Users", total)
                    col2.metric("Predicted Churn", churn_count)
                    col3.metric("Churn Rate", f"{churn_rate:.1%}")

                    fig = px.histogram(
                        result_df,
                        x="churn_probability",
                        nbins=20,
                        title="Churn Probability Distribution",
                        labels={"churn_probability": "Churn Probability"},
                        color_discrete_sequence=["#ef5350"],
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    st.dataframe(
                        result_df.sort_values("churn_probability", ascending=False),
                        use_container_width=True,
                    )

                    csv = result_df.to_csv(index=False).encode("utf-8")
                    st.download_button("Download Results CSV", csv, "churn_predictions.csv", "text/csv")

                except requests.exceptions.ConnectionError:
                    st.error(f"Cannot reach FastAPI at {FASTAPI_URL}")
                except Exception as e:
                    st.error(f"Error: {e}")

# ── API health ────────────────────────────────────────────────────────────────

st.divider()
with st.expander("API Health"):
    try:
        resp = requests.get(f"{FASTAPI_URL}/health", timeout=5)
        if resp.ok:
            st.success(f"API is healthy — {FASTAPI_URL}")
        else:
            st.warning(f"API returned {resp.status_code}")
    except Exception:
        st.error(f"API unreachable at {FASTAPI_URL}")
