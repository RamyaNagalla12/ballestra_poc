import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from snowflake.snowpark.functions import col, to_date
from snowflake.snowpark.context import get_active_session
import _snowflake
import json
import altair as alt
import time
from snowflake. snowpark. functions import col
import openai
from snowflake.snowpark.session import Session


# import streamlit as st

# --- temporary hardcoded users ---
USERS = {
    "client_a@mail.com": {"password": "a123", "role": "client_a"},
    "client_b@mail.com": {"password": "b123", "role": "client_b"},
    "mazzoni@mail.com": {"password": "m123", "role": "mazzoni"},
}

def show_login():
    st.set_page_config(page_title="Ballestra Login", layout="centered")

    st.markdown("""
        <style>
        .main {
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
        }
        .login-container {
            width: 320px;
            padding: 2rem;
            border-radius: 15px;
            background-color: #1c1e21;
            box-shadow: 0 0 15px rgba(0,0,0,0.4);
            text-align: center;
        }
        .stTextInput, .stPasswordInput {
            width: 100% !important;
        }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("<h3>Ballestra POC Login</h3>", unsafe_allow_html=True)
    email = st.text_input("Email")
    pwd = st.text_input("Password", type="password")

    if st.button("Login"):
        user = USERS.get(email)
        if user and user["password"] == pwd:
            st.session_state["logged_in"] = True
            st.session_state["role"] = user["role"]
            st.session_state["user_email"] = email
            st.success("Login successful ✅")
            st.rerun()
        else:
            st.error("Invalid email or password")

    st.markdown("</div>", unsafe_allow_html=True)
    
def require_login():
    if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
        show_login()
        st.stop()

def logout_button():
    if st.button("Logout"):
        st.session_state.clear()
        st.rerun()

# block all code until login
require_login()





# --- Page Config + Theme ---
st.set_page_config(page_title="BALLESTRA", layout="wide", page_icon="📊")
st.markdown("""
<style>
html, body, .reportview-container { background-color: #0f1115; color: #e6eef8; }
.stSidebar { background-color: #0b0d10; color: #e6eef8; }
.css-1d391kg, .stMetric, .dataframe { color: #e6eef8; }
</style>
""", unsafe_allow_html=True)



# --- Sidebar Controls ---
st.sidebar.title("POC Controls")


role = st.session_state["role"]



# Sidebar: role + dropdown
st.sidebar.markdown(
    '<div style="color:#808080; font-weight:600; font-size:18px; margin-bottom:6px;">Account Info:</div>',
    unsafe_allow_html=True
)

if role == "client_a":
    st.sidebar.markdown(
        '<div class="account-box" style="color:#808080;">You are logged in as Client A</div>',
        unsafe_allow_html=True
    )
    client_dropdown = "Client A"

elif role == "client_b":
    st.sidebar.markdown(
        '<div class="account-box" style="color:#808080;">You are logged in as Client B</div>',
        unsafe_allow_html=True
    )
    client_dropdown = "Client B"

elif role == "mazzoni":
    st.sidebar.markdown(
        '<div class="account-box">You are logged in as Mazzoni User</div>',
        unsafe_allow_html=True
    )
    client_dropdown = st.sidebar.selectbox(
        "Select Client View",
        ["Client A", "Client B", "Mazzoni"],
        key="client_selection",
    )
    


# Safety: block access
if client_dropdown == "Client A" and role not in ["client_a", "mazzoni"]:
    st.error("Access denied ❌")
    st.stop()
if client_dropdown == "Client B" and role not in ["client_b", "mazzoni"]:
    st.error("Access denied ❌")
    st.stop()
if client_dropdown == "Mazzoni" and role != "mazzoni":
    st.error("Access denied ❌")
    st.stop()











# --- Active Snowflake Session ---
session = get_active_session()



# --- Title + Period Filter ---
if client_dropdown == "Client A":
    st.title("Production and Quality")
elif client_dropdown == "Client B":
    st.title("Operations and Customer")
elif client_dropdown == "Mazzoni":
    st.title(" Mazzoni")
else:
    st.title("Dashboard")



period = st.selectbox("Select Period", ["Last 7 days", "Last 30 days", "Last 90 days", "All time"])
today = datetime.utcnow().date()
start_date = None
if period == "Last 7 days":
    start_date = today - timedelta(days=7)
elif period == "Last 30 days":
    start_date = today - timedelta(days=30)
elif period == "Last 90 days":
    start_date = today - timedelta(days=90)


st.markdown("""
<style>
div[data-baseweb="select"] {
    width: 180px !important;   /* adjust width (try 150–200px) */
}
</style>
""", unsafe_allow_html=True)


# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["Overview", "Consumption by time band", "Chatbot"])
logout_button()

# --- Load Data ---
try:
    if client_dropdown == "Client A":
        df = session.table("BALLESTRA.BALLESTRA.CLIENT_A_TABLE")
    elif client_dropdown == "Client B":
        df = session.table("BALLESTRA.BALLESTRA.CLIENT_B_TABLE")
    elif client_dropdown == "Mazzoni":
        # Load separate tables for A and B
        df_a = session.table("BALLESTRA.BALLESTRA.CLIENT_A_TABLE").to_pandas()
        df_b = session.table("BALLESTRA.BALLESTRA.CLIENT_B_TABLE").to_pandas()
        # Merge tables if there is a common column
        common_cols = [c for c in df_a.columns if c in df_b.columns]
        if common_cols:
            data = pd.merge(df_a, df_b, on=common_cols, how="outer")
        else:
            # If no common column, just concatenate vertically with keys
            df_a["Client"] = "A"
            df_b["Client"] = "B"
            data = pd.concat([df_a, df_b], ignore_index=True)
    else:
        df = pd.DataFrame()
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# --- Apply Date Filter ---
if client_dropdown != "Mazzoni":
    if start_date:
        for possible_col in ["TARGET_DATE", "MAINTENANCE_REQUEST_DATE", "EVENT_LOG_DATE"]:
            if possible_col in df.columns:
                df = df.filter((to_date(col(possible_col)) >= start_date) & (to_date(col(possible_col)) <= today))
                break
    try:
        data = df.to_pandas()
    except Exception as e:
        st.error(f"Error converting to pandas: {e}")
        data = pd.DataFrame()

if data.empty:
    st.warning("No data found for this client.")
else:
    # --- Tab 1: Overview ---
    with tab1:
        st.markdown("### KPI's")
        st.markdown("<br>", unsafe_allow_html=True)
        k1, k2, k3 = st.columns(3)
        st.markdown("""
        <style>
        div[data-testid="stMetric"] {
            background-color: rgba(255, 255, 255, 0.05);  /* more transparent */
            border: 1px solid rgba(255, 255, 255, 0.1);   /* plain thin border */
            padding: 15px;
            border-radius: 12px;
            text-align: center;
            transition: all 0.3s ease-in-out;
        }
        
        [data-testid="stMetricValue"], [data-testid="stMetricLabel"] {
            background: transparent !important;
        }
        </style>
        """, unsafe_allow_html=True)

        chart_df = None

        # st.markdown(f"### KPI's: {client_dropdown}")
        # k1, k2, k3 = st.columns(3)

    # Pick valid date column safely
        date_col = None
        for possible_col in ["TARGET_DATE", "MAINTENANCE_REQUEST_DATE", "EVENT_LOG_DATE"]:
            if possible_col in data.columns:
                date_col = possible_col
                break

        chart_df = None

        if client_dropdown == "Client A":
            prod_sum = data["PRODUCTION_QUANTITY"].sum() if "PRODUCTION_QUANTITY" in data.columns else 0
            avg_quality = 0
            if "QUALITY_STATUS" in data.columns:
                quality_map = {"PASS": 1, "FAIL": 0, "NOT_VALIDATED": None}
                data["QUALITY_NUM"] = data["QUALITY_STATUS"].map(quality_map)
                avg_quality = round(data["QUALITY_NUM"].mean(skipna=True) * 100, 1)
            total_plans = data["PRODUCTION_PLAN_ID"].nunique() if "PRODUCTION_PLAN_ID" in data.columns else 0
            k1.metric("Total Production", int(prod_sum))
            k2.metric("Avg Quality Pass Rate (%)", avg_quality)
            k3.metric("Total Plans", total_plans)


            st.markdown("<br>", unsafe_allow_html=True)

    

        # Chart
            if not data.empty and date_col in data.columns:
                data[date_col] = pd.to_datetime(data[date_col], errors="coerce")
                chart_df = (
                    data.groupby(data[date_col].dt.date)["PRODUCTION_QUANTITY"]
                    .sum()
                    .reset_index()
                    .rename(columns={date_col: "Date", "PRODUCTION_QUANTITY": "Production"})
                )
                chart_df = chart_df.sort_values("Date")

                if not chart_df.empty:
                    chart = (
                        alt.Chart(chart_df)
                        .mark_line(point=True, color="#5dade2")
                        .encode(
                            x=alt.X("Date:T", title="Date", axis=alt.Axis(format="%b %d", labelAngle=-30)),
                            y=alt.Y("Production:Q", title="Production Quantity"),
                            tooltip=["Date:T", "Production:Q"]
                        )
                        .properties(
                            title=alt.TitleParams(
                                text=" Production Quantity Trend",
                                fontSize=28,          # 🔹 Bigger title
                                # fontWeight="bold",    # 🔹 Bold title
                                anchor="start",       # 🔹 Align left
                            ),
                            width=800,
                            height=350
                        )
                    )
                    st.altair_chart(chart, use_container_width=True)
                    st.markdown("#### Data Table")
                    st.dataframe(data.head(200))
            else:
                st.info("No production data found to plot.")

        elif client_dropdown == "Client B":
            total_downtime = 0
            if "PRODUCTION_DOWN_TIME_HOURS" in data.columns:
                total_downtime += data["PRODUCTION_DOWN_TIME_HOURS"].fillna(0).sum()
            if "PRODUCTION_DOWN_TIME" in data.columns:
                total_downtime += data["PRODUCTION_DOWN_TIME"].fillna(0).sum()

            avg_downtime = round(total_downtime / len(data), 2) if len(data) > 0 else 0
            total_notifications = data["NOTIFICATION_ID"].nunique() if "NOTIFICATION_ID" in data.columns else 0

            k1.metric("Total Downtime (hrs)", total_downtime)
            k2.metric("Avg Downtime (hrs)", avg_downtime)
            k3.metric("Total Notifications", total_notifications)

            st.markdown("<br>", unsafe_allow_html=True)
            

        # --- KPI Line Chart: Maintenance & Notifications Trend ---

            if not data.empty and date_col in data.columns:
                df_chart = data.copy()
                df_chart[date_col] = pd.to_datetime(df_chart[date_col], errors="coerce")

                maint_trend = (
                    df_chart.groupby(df_chart[date_col].dt.date)["MAINTENANCE_ID"]
                    .nunique()
                    .reset_index()
                    .rename(columns={df_chart.columns[0]: "Date", "MAINTENANCE_ID": "Maintenance_Requests"})
                )

                notif_trend = (
                    df_chart.groupby(df_chart[date_col].dt.date)["NOTIFICATION_ID"]
                    .nunique()
                    .reset_index()
                    .rename(columns={df_chart.columns[0]: "Date", "NOTIFICATION_ID": "Notifications"})
                )

    # Fix rename issue safely
                if date_col in maint_trend.columns:
                    maint_trend = maint_trend.rename(columns={date_col: "Date"})
                if date_col in notif_trend.columns:
                    notif_trend = notif_trend.rename(columns={date_col: "Date"})

                merged_trend = pd.merge(maint_trend, notif_trend, on="Date", how="outer").fillna(0)
                trend_long = merged_trend.melt("Date", var_name="KPI", value_name="Count")
                trend_long = trend_long.sort_values("Date")

                chart = (
                    alt.Chart(trend_long)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("Date:T", title="Date", axis=alt.Axis(format="%b %d", labelAngle=-30)),
                        y=alt.Y("Count:Q", title="Count"),
                        color="KPI:N",
                        tooltip=["Date:T", "KPI:N", "Count:Q"]
                    )
                       .properties(
                            title=alt.TitleParams(
                                text=" Maintenance & Notification Trend",
                                fontSize=28,          # 🔹 Bigger title
                                # fontWeight="bold",    # 🔹 Bold title
                                anchor="start",       # 🔹 Align left
                            ),
                            width=800,
                            height=350
                        )
                    )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No valid data for KPI trend.")

        # --- Data Table ---
            st.markdown("#### Data Table")
            st.dataframe(data.head(200))

            
        elif client_dropdown == "Mazzoni":
             # ensure date_col exists
            if date_col in data.columns:
                data[date_col] = pd.to_datetime(data[date_col], errors="coerce")
                # compute window endpoints as Timestamps (based on the data itself)
                today_ts = pd.to_datetime(data[date_col].max()).normalize()
                if period == "Last 7 days":
                    start_ts = today_ts - pd.Timedelta(days=7)
                elif period == "Last 30 days":
                    start_ts = today_ts - pd.Timedelta(days=30)
                elif period == "Last 90 days":
                    start_ts = today_ts - pd.Timedelta(days=90)
                else:
                    start_ts = pd.to_datetime(data[date_col].min()).normalize()
                # filter using timestamps (both sides are Timestamp dtype)
                data = data[(data[date_col] >= start_ts) & (data[date_col] <= today_ts)]
            else:
                # if no date column, leave data as-is (already earlier warning)
                pass

            # --- Client A KPIs ---
            prod_sum_a = data["PRODUCTION_QUANTITY"].sum() if "PRODUCTION_QUANTITY" in data.columns else 0
            avg_quality_a = 0
            if "QUALITY_STATUS" in data.columns:
                quality_map = {"PASS": 1, "FAIL": 0, "NOT_VALIDATED": None}
                data["QUALITY_NUM"] = data["QUALITY_STATUS"].map(quality_map)
                avg_quality_a = round(data["QUALITY_NUM"].mean(skipna=True) * 100, 1)

            # --- Client B KPIs ---
            down_sum_b = data["PRODUCTION_DOWN_TIME_HOURS"].sum() if "PRODUCTION_DOWN_TIME_HOURS" in data.columns else 0
            avg_downtime_b = round(data["PRODUCTION_DOWN_TIME_HOURS"].mean(), 2) if "PRODUCTION_DOWN_TIME_HOURS" in data.columns else 0
            total_notifications_b = data["NOTIFICATION_ID"].nunique() if "NOTIFICATION_ID" in data.columns else 0

            k1.metric("Client A Total Production", int(prod_sum_a))
            k2.metric("Client A Avg Quality (%)", avg_quality_a)
            k3.metric("Client B Total Downtime (hrs)", down_sum_b)
            k4, k5 = st.columns(2)
            k4.metric("Client B Avg Downtime (hrs)", avg_downtime_b)
            k5.metric("Client B Total Notifications", total_notifications_b)


            st.markdown("<br><br>", unsafe_allow_html=True)

            
            # --- Trend chart (Altair clean version) ---
            if date_col in data.columns:
                df_chart = data.copy()
                df_chart[date_col] = pd.to_datetime(df_chart[date_col], errors="coerce")

                # Client A production trend
                prod_trend = (
                    df_chart.groupby(df_chart[date_col].dt.date)["PRODUCTION_QUANTITY"]
                    .sum()
                    .reset_index()
                    .rename(columns={df_chart.columns[0]: "Date", "PRODUCTION_QUANTITY": "Client A Production"})
                )

                # Client B notifications trend
                notif_trend = (
                    df_chart.groupby(df_chart[date_col].dt.date)["NOTIFICATION_ID"]
                    .nunique()
                    .reset_index()
                    .rename(columns={df_chart.columns[0]: "Date", "NOTIFICATION_ID": "Client B Notifications"})
                )

                # Normalize column names and types
                if date_col in prod_trend.columns:
                    prod_trend = prod_trend.rename(columns={date_col: "Date"})
                if date_col in notif_trend.columns:
                    notif_trend = notif_trend.rename(columns={date_col: "Date"})

                prod_trend["Date"] = pd.to_datetime(prod_trend["Date"])
                notif_trend["Date"] = pd.to_datetime(notif_trend["Date"])

                trend_df = pd.merge(prod_trend, notif_trend, on="Date", how="outer").fillna(0)
                trend_long = trend_df.melt("Date", var_name="KPI", value_name="Count")
                trend_long = trend_long.sort_values("Date")

                chart = (
                    alt.Chart(trend_long)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("Date:T", title="Date", axis=alt.Axis(format="%b %d", labelAngle=-30, labelOverlap=True, labelFlush=False)),
                        y=alt.Y("Count:Q", title="Count"),
                        color="KPI:N",
                        tooltip=["Date:T", "KPI:N", "Count:Q"]
                    )
                       .properties(
                            title=alt.TitleParams(
                                text="Client A & B Trends",
                                fontSize=28,          # 🔹 Bigger title
                                # fontWeight="bold",    # 🔹 Bold title
                                anchor="start",       # 🔹 Align left
                            ),
                            width=800,
                            height=350
                        )
                    )
                st.altair_chart(chart, use_container_width=True)

            st.markdown("#### Data Table")
            st.dataframe(data.head(200))
    # --- Tab 2: Consumption by time band ---
    with tab2:
        st.markdown("###  Consumption by Time Band")
        st.caption("Shows daily consumption split into 4 time bands: F1 00–06, F2 06–12, F3 12–18, F4 18–24.")
        st.markdown("<br>", unsafe_allow_html=True)

    # --- CLIENT A ---
        if client_dropdown == "Client A":
            if not data.empty and "PRODUCTION_QUANTITY" in data.columns and "TARGET_DATE" in data.columns:
                df_time = data.copy()
                df_time["TARGET_DATE"] = pd.to_datetime(df_time["TARGET_DATE"], errors="coerce")
                df_time = df_time.dropna(subset=["TARGET_DATE", "PRODUCTION_QUANTITY"])

            # --- Define Time Bands ---
                def get_time_band(hour):
                    if 0 <= hour < 6:
                        return "F1 00–06"
                    elif 6 <= hour < 12:
                        return "F2 06–12"
                    elif 12 <= hour < 18:
                        return "F3 12–18"
                    else:
                        return "F4 18–24"

                df_time["TimeBand"] = df_time["TARGET_DATE"].dt.hour.apply(get_time_band)
                df_time["Date"] = df_time["TARGET_DATE"].dt.date

            # --- Aggregate by Date + Band ---
                agg = (
                    df_time.groupby(["Date", "TimeBand"])["PRODUCTION_QUANTITY"]
                    .sum()
                    .reset_index()
                    .rename(columns={"PRODUCTION_QUANTITY": "Consumption_kWh"})
                )

                bands = ["F1 00–06", "F2 06–12", "F3 12–18", "F4 18–24"]
                row1_col1, row1_col2 = st.columns(2, gap="large")
                row2_col1, row2_col2 = st.columns(2, gap="large")
                cols = [row1_col1, row1_col2, row2_col1, row2_col2]

  
            # --- Charts ---
                for i, band in enumerate(bands):
                    chart_data = agg[agg["TimeBand"] == band].copy()
                    chart_data["Date"] = pd.to_datetime(chart_data["Date"], errors="coerce")

    # --- If empty, create dummy row to show empty chart ---
                    if chart_data.empty:
                        chart_data = pd.DataFrame({"Date": [pd.NaT], "Consumption_kWh": [0]})

                    chart_data = chart_data.sort_values("Date")

                    chart = (
                        alt.Chart(chart_data)
                        .mark_bar(color="#5dade2")
                        .encode(
                            x=alt.X("Date:T", title="Date", axis=alt.Axis(format="%b %d", labelAngle=-30)),
                            y=alt.Y("Consumption_kWh:Q", title="Consumption (kWh)"),
                            tooltip=["Date:T", "Consumption_kWh:Q"]
                        )
                        .properties(
                            title=f"{band} – Production Quantity as Consumption",
                            width=400,
                            height=280
                        )
                        .configure_title(anchor="start", fontSize=16, dy=5)
                    )

                    with cols[i]:
                        st.altair_chart(chart, use_container_width=True)
            else:
                st.warning("⚠️ Required columns not found for Client A (need TARGET_DATE and PRODUCTION_QUANTITY).")

    # --- CLIENT B ---
        elif client_dropdown == "Client B":
            df_time = data.copy()
            time_col = None
            for c in ["MAINTENANCE_REQUEST_DATE", "EVENT_LOG_DATE"]:
                if c in df_time.columns:
                    time_col = c
                    break

            if time_col and any(x in df_time.columns for x in ["PRODUCTION_DOWN_TIME_HOURS", "PRODUCTION_DOWN_TIME"]):
                df_time[time_col] = pd.to_datetime(df_time[time_col], errors="coerce")
                df_time["Consumption_Proxy"] = 0
            if "PRODUCTION_DOWN_TIME_HOURS" in df_time.columns:
                df_time["Consumption_Proxy"] += df_time["PRODUCTION_DOWN_TIME_HOURS"].fillna(0)
            if "PRODUCTION_DOWN_TIME" in df_time.columns:
                df_time["Consumption_Proxy"] += df_time["PRODUCTION_DOWN_TIME"].fillna(0)

        # --- Define Time Bands ---
            def get_time_band(hour):
                if 0 <= hour < 6:
                    return "F1 00–06"
                elif 6 <= hour < 12:
                    return "F2 06–12"
                elif 12 <= hour < 18:
                    return "F3 12–18"
                else:
                    return "F4 18–24"

            df_time["TimeBand"] = df_time[time_col].dt.hour.apply(get_time_band)
            df_time["Date"] = df_time[time_col].dt.date

        # --- Aggregate ---
            agg = (
                df_time.groupby(["Date", "TimeBand"])["Consumption_Proxy"]
                .sum()
                .reset_index()
                .rename(columns={"Consumption_Proxy": "Consumption_Hours"})
            )

            bands = ["F1 00–06", "F2 06–12", "F3 12–18", "F4 18–24"]
            row1_col1, row1_col2 = st.columns(2, gap="large")
            row2_col1, row2_col2 = st.columns(2, gap="large")
            cols = [row1_col1, row1_col2, row2_col1, row2_col2]

        # --- Charts (Altair clean version) ---
            for i, band in enumerate(bands):
                chart_data = agg[agg["TimeBand"] == band].copy()
                if not chart_data.empty:
                    chart_data["Date"] = pd.to_datetime(chart_data["Date"], errors="coerce")
                    chart_data = chart_data.sort_values("Date")

                chart = (
                    alt.Chart(chart_data)
                    .mark_bar()
                    .encode(
                        x=alt.X("Date:T", title="Date", axis=alt.Axis(format="%b %d", labelAngle=-30)),
                        y=alt.Y("Consumption_Hours:Q", title="Downtime (Hours)"),
                        tooltip=["Date:T", "Consumption_Hours:Q"],
                        color=alt.value("#1f77b4") 
                     )
                     .properties(
                         title=f"{band} – Downtime as Consumption Proxy",
                         width=400,
                         height=280
                     )
                     .configure_title(anchor="start", fontSize=16, dy=-5)
                )

                with cols[i]:
                    st.altair_chart(chart, use_container_width=True)
                
                
        elif client_dropdown == "Mazzoni":

# --- Load Data ---
            client_a_data = session.table("BALLESTRA.BALLESTRA.CLIENT_A_TABLE").to_pandas()
            client_b_data = session.table("BALLESTRA.BALLESTRA.CLIENT_B_TABLE").to_pandas()

# --- Apply Period Filter ---
            today = datetime.utcnow()
            if period == "Last 7 days":
                start_date = today - timedelta(days=7)
            elif period == "Last 30 days":
                start_date = today - timedelta(days=30)
            elif period == "Last 90 days":
                start_date = today - timedelta(days=90)
            else:
                start_date = None

# --- Client A ---
            df_a = client_a_data.copy()
            if not df_a.empty and "TARGET_DATE" in df_a.columns and "PRODUCTION_QUANTITY" in df_a.columns:
                df_a["TARGET_DATE"] = pd.to_datetime(df_a["TARGET_DATE"], errors="coerce")
                if start_date is not None:
                    df_a = df_a[(df_a["TARGET_DATE"] >= start_date) & (df_a["TARGET_DATE"] <= today)]

                df_a["Date"] = df_a["TARGET_DATE"].dt.date
                df_a["Hour"] = df_a["TARGET_DATE"].dt.hour

                def get_time_band(hour):
                    if 0 <= hour < 6:
                        return "F1 00–06"
                    elif 6 <= hour < 12:
                        return "F2 06–12"
                    elif 12 <= hour < 18:
                        return "F3 12–18"
                    else:
                        return "F4 18–24"

                df_a["TimeBand"] = df_a["Hour"].apply(get_time_band)
                df_a = (
                    df_a.groupby(["Date", "TimeBand"])["PRODUCTION_QUANTITY"]
                    .sum()
                    .reset_index()
                    .rename(columns={"PRODUCTION_QUANTITY": "Consumption"})
                )
                df_a["Client"] = "Client A"
            else:
                df_a = pd.DataFrame(columns=["Date", "TimeBand", "Consumption", "Client"])

# --- Client B ---
            df_b = client_b_data.copy()
            time_col = None
            for c in ["MAINTENANCE_REQUEST_DATE", "EVENT_LOG_DATE"]:
                if c in df_b.columns:
                    time_col = c
                    break

            if not df_b.empty and time_col:
                df_b[time_col] = pd.to_datetime(df_b[time_col], errors="coerce")
                if start_date is not None:
                    df_b = df_b[(df_b[time_col] >= start_date) & (df_b[time_col] <= today)]

                df_b["Date"] = df_b[time_col].dt.date
                df_b["Hour"] = df_b[time_col].dt.hour
                df_b["TimeBand"] = df_b["Hour"].apply(get_time_band)

                df_b["Consumption"] = 0
                if "PRODUCTION_DOWN_TIME_HOURS" in df_b.columns:
                    df_b["Consumption"] += df_b["PRODUCTION_DOWN_TIME_HOURS"].fillna(0)
                if "PRODUCTION_DOWN_TIME" in df_b.columns:
                    df_b["Consumption"] += df_b["PRODUCTION_DOWN_TIME"].fillna(0)

                df_b = (
                    df_b.groupby(["Date", "TimeBand"])["Consumption"]
                    .sum()
                    .reset_index()
                )
                df_b["Client"] = "Client B"
            else:
                df_b = pd.DataFrame(columns=["Date", "TimeBand", "Consumption", "Client"])

# --- Combine ---
            combined = pd.concat([df_a, df_b], ignore_index=True)
            # st.write("🔍 Debug TimeBand counts:", combined["TimeBand"].value_counts())
            # st.write("📅 Date range:", combined["Date"].min(), "→", combined["Date"].max())
            combined["Date"] = pd.to_datetime(combined["Date"])

            

# --- Charts ---
            combined["ValueLabel"] = combined.apply(
                lambda x: "Consumption (kWh)" if x["Client"] == "Client A" else "No. of Hours",
                axis=1
            )
            if not combined.empty:
                bands = ["F1 00–06", "F2 06–12", "F3 12–18", "F4 18–24"]
                rows = [st.columns(2), st.columns(2)]
                cols = [rows[0][0], rows[0][1], rows[1][0], rows[1][1]]

                for i, band in enumerate(bands):
                    chart_data = combined[combined["TimeBand"] == band]
                    if not chart_data.empty:
                        y_title = "Consumption (kWh)" if "Client A" in chart_data["Client"].values else "No. of Hours"

                        chart = (
                            alt.Chart(chart_data.sort_values("Date"))
                            .mark_bar()
                            .encode(
                                x=alt.X(
                                    "Date:T",
                                    title="Date",
                                    axis=alt.Axis(format="%b %d", labelAngle=-30)
                                ),
                                y=alt.Y("Consumption:Q", title=y_title),
                                color=alt.Color("Client:N", title="Client"),
                               tooltip=[
                                   "Client:N",
                                   "Date:T",
                                   alt.Tooltip("Consumption:Q", title=None),
                                   alt.Tooltip("ValueLabel:N", title="Metric")
                                ]
                            )
                            .properties(
                                title=f"{band} – Client A vs Client B",
                                width=400,
                                height=280
                            )
                            .configure_title(
                                anchor="start",    
                                fontSize=16,
                                # orient="bottom",  
                                dy=10       
                            )
                        )
                        with cols[i]:
                            st.altair_chart(chart, use_container_width=True)
                    else:
                        with cols[i]:
                            st.info(f"No data for {band}")

                    
# --- Tab 3: Generalized Cortex Analyst ---
with tab3:
    col1, clear_col2 = st.columns([6, 1])
    with col1:
        st.markdown("### Chatbot")

    # Initialize chat state
    if "last_role" not in st.session_state or st.session_state.last_role != role:
        st.session_state.messages = []
        st.session_state.active_suggestion = None
        st.session_state.form_submitted = {}
        st.session_state.warnings = []
        st.session_state.last_role = role  # track who logged in

# --- Initialize chat state if missing ---
    if "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.active_suggestion = None
        st.session_state.form_submitted = {}
        st.session_state.warnings = []
 
    # --- Map clients to semantic models ---
    client_model_map = {
        "Client A": "BALLESTRA.BALLESTRA.BALLESTRA_CSV_FILES/CLIENT_A.yaml",
        "Client B": "BALLESTRA.BALLESTRA.BALLESTRA_CSV_FILES/CLIENT_B.yaml",
        "Mazzoni": "BALLESTRA.BALLESTRA.BALLESTRA_CSV_FILES/MAZZ.yaml"
    }


    if role == "client_a":
        st.session_state.selected_semantic_model_path = client_model_map["Client A"]

    elif role == "client_b":
        st.session_state.selected_semantic_model_path = client_model_map["Client B"]

    elif role == "mazzoni":
        model_labels = {
            "BALLESTRA.BALLESTRA.BALLESTRA_CSV_FILES/CLIENT_A.yaml": "Client A Insights",
            "BALLESTRA.BALLESTRA.BALLESTRA_CSV_FILES/CLIENT_B.yaml": "Client B Insights",
            "BALLESTRA.BALLESTRA.BALLESTRA_CSV_FILES/MAZZONI.yaml": "Mazzoni Overview"
        }
        selected_label = st.sidebar.selectbox(
            "Select Semantic Model:",
            options=list(model_labels.values()),
            index=0,
            key="mazzoni_model_select"
        )
        st.session_state.selected_semantic_model_path = [
            k for k, v in model_labels.items() if v == selected_label
        ][0]
    else:
        st.warning("Unknown role. Please log in again.")
 
    # --- User input ---
    user_input = st.chat_input("Ask a question about this client:")
    _, clear_col = st.columns([5, 1])
    with clear_col:
        if st.button("Clear", key="clear_chat", use_container_width=True):
            st.session_state.messages = []
            st.session_state.active_suggestion = None
            st.session_state.form_submitted = {}
            st.session_state.warnings = []
            st.toast("Chat history cleared!", icon="🧹")
            st.rerun()
            
    if "chat_session_id" not in st.session_state or st.session_state.last_role != role:
        st.session_state.chat_session_id = f"{role}_{int(time.time())}"
        st.session_state.last_role = role

    if user_input:
        st.session_state.messages.append({"role": "user", "content": [{"type": "text", "text": user_input}]})
 
        with st.chat_message("analyst"):
            with st.spinner("Analyst is responding..."):
                request_body = {
                    "messages": st.session_state.messages,
                    "semantic_model_file": f"@{st.session_state.selected_semantic_model_path}",
                    "session_id": st.session_state.chat_session_id  # important to isolate context
                }
                try:
                    resp = _snowflake.send_snow_api_request(
                        "POST",
                        "/api/v2/cortex/analyst/message",
                        {}, {}, request_body, None, 50000
                    )
                    parsed_content = json.loads(resp.get("content", "{}"))
                    if resp.get("status", 500) < 400:
                        analyst_content = parsed_content.get("message", {}).get("content", [])
                    else:
                        analyst_content = [{"type": "text", "text": f"Error: {parsed_content.get('error_code', 'Unknown')}"}]
                except Exception as e:
                    analyst_content = [{"type": "text", "text": f"Exception: {e}"}]
 
                st.session_state.messages.append({"role": "analyst", "content": analyst_content})
 
    # --- Display chat messages ---
    for msg_idx, msg in enumerate(st.session_state.messages):
        role = msg["role"]
        content = msg.get("content", [])
        with st.chat_message(role):
            for idx, item in enumerate(content):
                if item.get("type") == "text":
                    st.markdown(item.get("text", ""))
                elif item.get("type") == "suggestions":
                    for s_idx, suggestion in enumerate(item.get("suggestions", [])):
                        key = f"sugg_{msg_idx}_{idx}_{s_idx}"
                        if st.button(suggestion, key=key):
                            st.session_state.active_suggestion = suggestion
                elif item.get("type") == "sql":
                    sql_query = item.get("statement")
                    confidence = item.get("confidence")
                    with st.expander("SQL Query"):
                        st.code(sql_query, language="sql")
                    df, err_msg = None, None
                    try:
                        df = session.sql(sql_query).to_pandas()
                    except Exception as e:
                        err_msg = str(e)
                    if df is not None and not df.empty:
                        data_tab, chart_tab = st.tabs(["Data", "Chart"])
                        with data_tab:
                            st.dataframe(df)
                        with chart_tab:
                            if len(df.columns) >= 2:
                                x_col = df.columns[0]
                                y_col = df.columns[1]
                                st.line_chart(df.set_index(x_col)[y_col])
                    elif err_msg:
                        st.error(f"Error executing SQL: {err_msg}")
 
    # --- Handle suggestions ---
    if st.session_state.get("active_suggestion"):
        st.session_state.messages.append({
            "role": "user",
            "content": [{"type": "text", "text": st.session_state.active_suggestion}]
        })
        st.session_state.active_suggestion = None
        st.rerun()


  