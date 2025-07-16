import streamlit as st
import pandas as pd
from datetime import datetime
from snowflake.snowpark.functions import col

# -----------------------------------------------
# 🔗 Snowflake Connection
# -----------------------------------------------
cnx = st.connection("snowflake")
session = cnx.session()

# -----------------------------------------------
# 📦 Data Loaders
# -----------------------------------------------
@st.cache_data
def load_exercises():
    return session.table("EXERCISES").to_pandas()

@st.cache_data
def load_user_logs(username):
    return session.table("CALORIE_LOG") \
        .filter(col("USERNAME") == username) \
        .sort(col("LOG_TIMESTAMP"), ascending=False) \
        .to_pandas()

# -----------------------------------------------
# 🧠 Calorie Calculation Logic
# -----------------------------------------------
def calculate_calories(met: float, weight: float, duration: int) -> float:
    return round(met * weight * (duration / 60), 2)

def get_met_from_calories_per_min(cal_per_min):
    return round(cal_per_min / 3.5, 2)

# -----------------------------------------------
# 📝 Logging Function
# -----------------------------------------------
def log_calorie_entry(username, exercise, duration, weight, met, calories):
    now = datetime.utcnow().isoformat()

    data = pd.DataFrame([{
        "USERNAME": username,
        "LOG_TIMESTAMP": now,
        "EXERCISE_NAME": exercise,
        "DURATION_MINUTES": duration,
        "USER_WEIGHT_KG": weight,
        "MET_VALUE": met,
        "CALORIES_BURNED": calories
    }])

    session.write_pandas(data, table_name="CALORIE_LOG", overwrite=False)

# -----------------------------------------------
# 📈 UI: History Section
# -----------------------------------------------
def show_history(log_df, username):
    if not log_df.empty:
        st.subheader("📈 Your Calorie Burn History")

        st.metric("🔥 Total Calories Burned", f"{log_df['CALORIES_BURNED'].sum():.2f}")
        st.dataframe(log_df[["LOG_TIMESTAMP", "EXERCISE_NAME", "DURATION_MINUTES", "CALORIES_BURNED"]])
        
        chart_data = log_df.sort_values("LOG_TIMESTAMP")[["LOG_TIMESTAMP", "CALORIES_BURNED"]].set_index("LOG_TIMESTAMP")
        st.line_chart(chart_data)

        csv_data = log_df.to_csv(index=False)
        st.download_button(
            label="📥 Download Log as CSV",
            data=csv_data,
            file_name=f"{username}_calorie_log.csv",
            mime="text/csv"
        )
    else:
        st.info("No logs found. Run a calculation to log your first session!")

# -----------------------------------------------
# 🚀 App Layout
# -----------------------------------------------
def main():
    st.title("🔥 Calorie Burn Tracker with History")

    username = st.text_input("Enter your name:", value="guest")
    weight = st.number_input("Your weight (kg)", min_value=30.0, max_value=200.0, value=70.0)

    df_ex = load_exercises()
    exercise = st.selectbox("Select an Exercise", df_ex["EXERCISE_NAME"])
    duration = st.slider("Duration in minutes", 1, 120, 30)

    if st.button("Calculate and Log"):
        cal_per_min = df_ex[df_ex["EXERCISE_NAME"] == exercise]["CALORIES_PER_MINUTE"].values[0]
        met = get_met_from_calories_per_min(cal_per_min)
        calories = calculate_calories(met, weight, duration)

        st.success(f"{username}, you burned approx **{calories:.2f} calories** doing **{exercise}** for {duration} min.")
        log_calorie_entry(username, exercise, duration, weight, met, calories)
        st.toast("Entry logged!")

    log_df = load_user_logs(username)
    show_history(log_df, username)

# -----------------------------------------------
# 🏁 Run the App
# -----------------------------------------------
if __name__ == "__main__":
    main()

# -----------------------------------------------
# 🔒 Optional: Debug Secrets (Dev only)
# -----------------------------------------------
if st.sidebar.checkbox("🔍 Show Secrets (Dev Only)"):
    try:
        st.sidebar.json(st.secrets["snowflake"])
    except Exception as e:
        st.sidebar.error("❌ Could not load Snowflake secrets.")
        st.sidebar.exception(e)
