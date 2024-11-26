import os
import datetime
import pandas as pd
import streamlit as st

# Set Streamlit page config
st.set_page_config(page_title="The Reef", page_icon=":ocean:")

# App title
st.title(":ocean: The Reef")

# Fetch data from CSV
csv_file = 'reefpaidmedia.csv'

# Initialize session state
if "df" not in st.session_state:
    if os.path.exists(csv_file):
        st.session_state.df = pd.read_csv(csv_file)
    else:
        st.session_state.df = pd.DataFrame(columns=["Client_ID", "Account_ID", "Data_Source_Name", "Client_Name", "Campaign_ID", "Campaign_Name"])

# Section to add a new ticket
st.header("Add an Account")
with st.form("add_ticket_form"):
    account_id = st.text_area("Account ID")
    client_id = st.text_area("Client ID")
    data_source_name = st.selectbox("Data Source", ["Google Ads", "Microsoft Ads", "Facebook Ads"])
    Client_Name = st.text_area("Client Name")
    Campaign_ID = st.text_area("Campaign ID")
    Campaign_Name = st.text_area("Campaign Name")
    submitted = st.form_submit_button("Submit")

if submitted:
    # Generate new ticket
    df_new = pd.DataFrame(
        [{
            "Client_ID": client_id,
            "Account_ID": account_id,
            "Data_Source_Name": data_source_name,
            "Client_Name": Client_Name,
            "Campaign_ID": Campaign_ID,
            "Campaign_Name": Campaign_Name,
        }]
    )
    st.write("Ticket submitted! Here are the ticket details:")
    st.dataframe(df_new, use_container_width=True, hide_index=True)

    # Update session state and save to CSV
    st.session_state.df = pd.concat([st.session_state.df, df_new], axis=0, ignore_index=True)
    st.session_state.df.to_csv(csv_file, index=False)

# Show and edit existing tickets
st.header("The Reef")
st.write(f"Number of tickets: `{len(st.session_state.df)}`")
st.info(
    "You can edit the tickets by double-clicking on a cell. Note how the plots below "
    "update automatically! You can also sort the table by clicking on the column headers.",
    icon="✍️"
)

# Editable table
edited_df = st.data_editor(
    st.session_state.df,
    use_container_width=True,
    hide_index=True,
)

# Save changes back to CSV
if not edited_df.equals(st.session_state.df):
    st.session_state.df = edited_df
    st.session_state.df.to_csv(csv_file, index=False)
