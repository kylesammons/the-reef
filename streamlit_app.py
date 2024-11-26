import datetime
import random

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# Show app title and description.
st.set_page_config(page_title="The Reef", page_icon="🎫")
st.title("🎫 The Reef")

# Create the BigQuery client
credentials = service_account.Credentials.from_service_account_file('/Users/Trimark/Desktop/Jupyter_Notebooks/trimark-tdp-87c89fbd0816.json')
client = bigquery.Client(credentials=credentials, project='trimark-tdp')

# Fetch data from BigQuery
query = """
  SELECT
    *
  FROM
   `trimark-tdp.reef.paid_media`


"""
df = client.query(query).to_dataframe()

# Create a random Pandas dataframe with existing tickets.
if "df" not in st.session_state:


    # Save the dataframe in session state (a dictionary-like object that persists across
    # page runs). This ensures our data is persisted when the app updates.
    st.session_state.df = df


# Show a section to add a new ticket.
st.header("Add an Account")

# We're adding tickets via an `st.form` and some input widgets. If widgets are used
# in a form, the app will only rerun once the submit button is pressed.
with st.form("add_ticket_form"):
    account_id = st.text_area("Account ID")
    client_id = st.text_area("Client ID")
    data_source_name = st.selectbox("Data Source", ["Google Ads", "Microsoft Ads", "Facebook Ads"])
    submitted = st.form_submit_button("Submit")

if submitted:
    # Make a dataframe for the new ticket and append it to the dataframe in session
    # state.
    recent_ticket_number = int(max(st.session_state.df.ID).split("-")[1])
    today = datetime.datetime.now().strftime("%m-%d-%Y")
    df_new = pd.DataFrame(
        [
            {
                "Client ID": client_id,
                "Account ID": account_id,
                "Status": "Open",
                "Data Source": priority,
                "Date Submitted": today,
            }
        ]
    )

    # Show a little success message.
    st.write("Ticket submitted! Here are the ticket details:")
    st.dataframe(df_new, use_container_width=True, hide_index=True)
    st.session_state.df = pd.concat([df_new, st.session_state.df], axis=0)

# Show section to view and edit existing tickets in a table.
st.header("The Reef")
st.write(f"Number of tickets: `{len(st.session_state.df)}`")

st.info(
    "You can edit the tickets by double clicking on a cell. Note how the plots below "
    "update automatically! You can also sort the table by clicking on the column headers.",
    icon="✍️",
)

# Show the tickets dataframe with `st.data_editor`. This lets the user edit the table
# cells. The edited data is returned as a new dataframe.
edited_df = st.data_editor(
    st.session_state.df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Status": st.column_config.SelectboxColumn(
            "Status",
            help="Ticket status",
            options=["Open", "In Progress", "Closed"],
            required=True,
        ),
        "Priority": st.column_config.SelectboxColumn(
            "Priority",
            help="Priority",
            options=["High", "Medium", "Low"],
            required=True,
        ),
    },
    # Disable editing the ID and Date Submitted columns.
    disabled=["Date Submitted"],
)
