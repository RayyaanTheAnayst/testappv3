import streamlit as st
from snowflake.snowpark import Session

@st.cache_resource
def create_session():
    params = {
        "account": st.secrets["snowflake"]["account"].lower(),
        "user": st.secrets["snowflake"]["user"],
        "password": st.secrets["snowflake"]["password"],
        "role": st.secrets["snowflake"]["role"],
        "warehouse": st.secrets["snowflake"]["warehouse"],
        "database": st.secrets["snowflake"]["database"],
        "schema": st.secrets["snowflake"]["schema"],
    }
    st.write({k: v for k, v in params.items() if k != "password"})
    return Session.builder.configs(params).create()

try:
    session = create_session()
    st.success("Connected to Snowflake successfully!")
except Exception as e:
    st.error("Failed to connect to Snowflake:")
    st.exception(e)
