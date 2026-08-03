import os
import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector

# Page Config
st.set_page_config(
    page_title="Flight Operations Analytics",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling
st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
    }
    .stMetric {
        background-color: #1e222d;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #2e3440;
    }
    .stMetric label {
        color: #8892b0 !important;
    }
    </style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)
def fetch_data():
    """Fetch flight KPIs from Snowflake."""
    # Attempt to read credentials from Streamlit secrets first, then environment variables
    sf_user = st.secrets.get("SNOWFLAKE_USER", os.getenv("POSTGRES_USER", "Icedoutchirag"))
    sf_password = st.secrets.get("SNOWFLAKE_PASSWORD", os.getenv("POSTGRES_PASSWORD", ""))
    sf_account = st.secrets.get("SNOWFLAKE_ACCOUNT", "IJOTPCJ-UK61129")
    sf_warehouse = st.secrets.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    sf_database = st.secrets.get("SNOWFLAKE_DATABASE", "FLIGHT_DB")
    sf_schema = st.secrets.get("SNOWFLAKE_SCHEMA", "PUBLIC")
    sf_role = st.secrets.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema,
        role=sf_role
    )
    
    query = """
    SELECT 
        WINDOW_START,
        ORIGIN_COUNTRY,
        TOTAL_FLIGHTS,
        AVG_VELOCITY,
        ON_GROUND,
        LOAD_TIME
    FROM FLIGHT_KPIS
    ORDER BY TOTAL_FLIGHTS DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Header
st.title("✈️ Global Flight Operations Dashboard")
st.markdown("Real-time Medallion Pipeline Analytics powered by **Apache Airflow** & **Snowflake**")
st.divider()

try:
    df = fetch_data()
    
    # Calculate Summary Metrics
    total_flights = int(df["TOTAL_FLIGHTS"].sum())
    total_countries = len(df["ORIGIN_COUNTRY"].unique())
    avg_global_velocity = float(df["AVG_VELOCITY"].mean())
    total_on_ground = int(df["ON_GROUND"].sum())
    
    # Top KPI Metrics Row
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("🌍 Total Active Flights", f"{total_flights:,}")
    m2.metric("🚩 Active Countries", f"{total_countries}")
    m3.metric("⚡ Avg Aircraft Speed", f"{avg_global_velocity:.1f} m/s")
    m4.metric("🛬 Total Aircraft On Ground", f"{total_on_ground:,}")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Charts Row
    c1, c2 = st.columns([3, 2])
    
    with c1:
        st.subheader("Top 15 Countries by Active Flights")
        top_15 = df.head(15)
        fig_bar = px.bar(
            top_15,
            x="TOTAL_FLIGHTS",
            y="ORIGIN_COUNTRY",
            orientation="h",
            color="AVG_VELOCITY",
            color_continuous_scale="Viridis",
            labels={"TOTAL_FLIGHTS": "Total Flights", "ORIGIN_COUNTRY": "Country", "AVG_VELOCITY": "Avg Speed (m/s)"},
            template="plotly_dark"
        )
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig_bar, use_container_width=True)
        
    with c2:
        st.subheader("Air vs. Ground Breakdown")
        df_air_ground = pd.DataFrame({
            "Status": ["In Air", "On Ground"],
            "Count": [total_flights - total_on_ground, total_on_ground]
        })
        fig_pie = px.pie(
            df_air_ground,
            names="Status",
            values="Count",
            color="Status",
            color_discrete_map={"In Air": "#3b82f6", "On Ground": "#f59e0b"},
            hole=0.4,
            template="plotly_dark"
        )
        fig_pie.update_layout(margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig_pie, use_container_width=True)
        
    st.divider()
    
    # Detailed Data Table View
    st.subheader("📋 Flight Operations Data Stream")
    
    # Filter by Country Search
    search_country = st.text_input("🔍 Search Country:", "")
    if search_country:
        filtered_df = df[df["ORIGIN_COUNTRY"].str.contains(search_country, case=False, na=False)]
    else:
        filtered_df = df
        
    st.dataframe(
        filtered_df,
        column_config={
            "WINDOW_START": "Window Start",
            "ORIGIN_COUNTRY": "Origin Country",
            "TOTAL_FLIGHTS": st.column_config.NumberColumn("Total Flights", format="%d"),
            "AVG_VELOCITY": st.column_config.NumberColumn("Avg Speed (m/s)", format="%.2f"),
            "ON_GROUND": st.column_config.NumberColumn("On Ground", format="%d"),
            "LOAD_TIME": "Last Synchronized"
        },
        hide_index=True,
        use_container_width=True
    )

except Exception as e:
    st.error(f"Failed to connect to Snowflake or fetch data: {e}")
    st.info("Ensure your Snowflake credentials are configured in `.streamlit/secrets.toml` or environment variables.")
