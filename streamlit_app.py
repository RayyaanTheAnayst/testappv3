import streamlit as st
from snowflake.snowpark import Session

@st.cache_resource
def create_session():
    params = {
    "user": st.secrets["connections"]["snowflake"]["user"],
    "password": st.secrets["connections"]["snowflake"]["password"],
    "account": st.secrets["connections"]["snowflake"]["account"],
    "warehouse": st.secrets["connections"]["snowflake"]["warehouse"],
    "database": st.secrets["connections"]["snowflake"]["database"],
    "schema": st.secrets["connections"]["snowflake"]["schema"],
    "role": st.secrets["connections"]["snowflake"]["role"],
    }
    st.write({k: v for k, v in params.items() if k != "password"})
    return Session.builder.configs(params).create()

try:
    session = create_session()
    st.success("Connected to Snowflake successfully!")
except Exception as e:
    st.error("Failed to connect to Snowflake:")
    st.exception(e)
