import os
import requests
import pandas as pd

def load_gold_to_snowflake(**context):
    gold_file = context["ti"].xcom_pull(
        key="gold_file",
        task_ids="gold_aggregate"
    )

    if not gold_file:
        raise ValueError("Gold file not found in xcom")
    
    execution_date = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
    
    df = pd.read_csv(gold_file)

    # 1. Primary: Load into Supabase (Free Forever Cloud Database)
    supabase_url = os.getenv("SUPABASE_URL", "https://ybxbqdukhfjnbwcmwdih.supabase.co")
    supabase_key = os.getenv("SUPABASE_KEY", "sb_publishable_XBxayHSnweLtvY7s8hICRA_hibV0b1W")

    if supabase_url and supabase_key:
        records = []
        for _, row in df.iterrows():
            avg_vel = float(row["avg_velocity"]) if pd.notnull(row["avg_velocity"]) else 0.0
            records.append({
                "window_start": execution_date,
                "origin_country": str(row["origin_country"]),
                "total_flights": int(row["total_flights"]),
                "avg_velocity": avg_vel,
                "on_ground": int(row["on_ground"])
            })

        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }

        try:
            res = requests.post(
                f"{supabase_url}/rest/v1/flight_kpis?on_conflict=window_start,origin_country",
                headers=headers,
                json=records,
                timeout=30
            )
            if res.status_code in [200, 201, 204]:
                print("✅ Successfully upserted flight KPIs into Supabase (Free Forever Database)!")
            else:
                print(f"Supabase response status {res.status_code}: {res.text}")
        except Exception as e:
            print(f"Supabase loading notice: {e}")

    # 2. Secondary: Load into Snowflake (if active credentials present)
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection("flight_snowflake")
        user = conn.login
        password = conn.password
        account = conn.extra_dejson["account"]
        warehouse = conn.extra_dejson.get("warehouse")
        database = conn.extra_dejson.get("database")
        schema = conn.schema
        role = conn.extra_dejson.get("role")
    except Exception:
        user = os.getenv("SNOWFLAKE_USER")
        password = os.getenv("SNOWFLAKE_PASSWORD")
        account = os.getenv("SNOWFLAKE_ACCOUNT")
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
        database = os.getenv("SNOWFLAKE_DATABASE", "FLIGHT_DB")
        schema = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
        role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

    if account and user and password:
        account = account.replace("https://", "").replace("http://", "").split(".")[0].strip()
        try:
            import snowflake.connector
            sf_conn = snowflake.connector.connect(
                user=user,
                password=password,
                account=account,
                warehouse=warehouse,
                database=database,
                schema=schema,
                role=role
            )

            merge_sql = """
                MERGE INTO FLIGHT_KPIS tgt
                USING (
                    SELECT
                        TO_TIMESTAMP(%s) AS WINDOW_START,
                        %s AS ORIGIN_COUNTRY,
                        %s AS TOTAL_FLIGHTS,
                        %s AS AVG_VELOCITY,
                        %s AS ON_GROUND
                ) src
                ON tgt.WINDOW_START = src.WINDOW_START
                   AND tgt.ORIGIN_COUNTRY = src.ORIGIN_COUNTRY
                WHEN MATCHED THEN UPDATE SET
                    TOTAL_FLIGHTS = src.TOTAL_FLIGHTS,
                    AVG_VELOCITY = src.AVG_VELOCITY,
                    ON_GROUND = src.ON_GROUND,
                    LOAD_TIME = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT
                (WINDOW_START, ORIGIN_COUNTRY, TOTAL_FLIGHTS, AVG_VELOCITY, ON_GROUND)
                VALUES
                (src.WINDOW_START, src.ORIGIN_COUNTRY, src.TOTAL_FLIGHTS, src.AVG_VELOCITY, src.ON_GROUND);
            """

            with sf_conn.cursor() as cursor:
                for _, row in df.iterrows():
                    cursor.execute(
                        merge_sql,
                        (
                            execution_date,
                            row["origin_country"],
                            int(row["total_flights"]),
                            float(row["avg_velocity"]),
                            int(row["on_ground"]),
                        ),
                    )

            sf_conn.close()
            print("✅ Successfully upserted flight KPIs into Snowflake!")
        except Exception as e:
            print(f"Snowflake connection bypassed: {e}")