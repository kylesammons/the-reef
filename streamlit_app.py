import datetime
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# Set Streamlit page config
st.set_page_config(page_title="The Reef", page_icon="🎫")

# App title
st.title("🎫 The Reef")

# Create the BigQuery client
credentials = service_account.Credentials.from_service_account_file(
    '/Users/Trimark/Desktop/Jupyter_Notebooks/trimark-tdp-87c89fbd0816.json'
)
client = bigquery.Client(credentials=credentials, project='trimark-tdp')

# Fetch data from BigQuery
query = """
  SELECT *
  FROM `trimark-tdp.reef.paid_media`
LIMIT 100"""
df = client.query(query).to_dataframe()


# Initialize session state
if "df" not in st.session_state:
    st.session_state.df = df

# Section to add a new ticket
st.header("Add an Account")
with st.form("add_ticket_form"):
    account_id = st.text_area("Account ID")
    client_id = st.text_area("Client ID")
    data_source_name = st.selectbox("Data Source", ["Google Ads", "Microsoft Ads", "Facebook Ads"])
    submitted = st.form_submit_button("Submit")

if submitted:
    # Generate new ticket
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    df_new = pd.DataFrame(
        [{
            "Client ID": client_id,
            "Account ID": account_id,
            "Status": "Open",
            "Priority": "Medium",
            "Data Source": data_source_name,
            "Date Submitted": today,
        }]
    )
    st.write("Ticket submitted! Here are the ticket details:")
    st.dataframe(df_new, use_container_width=True, hide_index=True)

    # Update session state
    st.session_state.df = pd.concat([df_new, st.session_state.df], axis=0)
