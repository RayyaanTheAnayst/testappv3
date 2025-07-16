import streamlit as st
from snowflake.snowpark import Session

@st.cache_resource
def create_session():
    params = {
        "account": st.secrets["conections.snowflake"]["account"].lower(),
        "user": st.secrets["conections.snowflake"]["user"],
        "password": st.secrets["conections.snowflake"]["password"],
        "role": st.secrets["conections.snowflake"]["role"],
        "warehouse": st.secrets["conections.snowflake"]["warehouse"],
        "database": st.secrets["conections.snowflake"]["database"],
        "schema": st.secrets["conections.snowflake"]["schema"],
    }
    st.write({k: v for k, v in params.items() if k != "password"})
    return Session.builder.configs(params).create()

try:
    session = create_session()
    st.success("Connected to Snowflake successfully!")
except Exception as e:
    st.error("Failed to connect to Snowflake:")
    st.exception(e)
