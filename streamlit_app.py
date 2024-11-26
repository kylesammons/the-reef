import os
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
    # Select Client Name
    client_name = st.selectbox("Client Name", st.session_state.df["Client_Name"].unique())
    
    # Dynamically get Client_ID based on selected Client_Name but hide it
    client_ids = st.session_state.df[st.session_state.df["Client_Name"] == client_name]["Client_ID"].unique()
    st.session_state.client_id = client_ids[0] if len(client_ids) > 0 else None

    # Use st.text_input for a shorter height Account ID
    account_id = st.text_input("Account ID")  # Single-line input

    data_source_name = st.selectbox("Data Source", ["Google Ads", "Microsoft Ads", "Facebook Ads"])
    
    # Conditionally display campaign fields only for "Window World" client and "Facebook Ads" data source
    if client_name == "Window World" and data_source_name == "Facebook Ads":
        campaign_id = st.text_area("Campaign ID")
        campaign_name = st.text_area("Campaign Name")
    else:
        campaign_id = None
        campaign_name = None

    submitted = st.form_submit_button("Submit")

if submitted:
    # Use the dynamically populated client_id from session state
    client_id = st.session_state.client_id
    
    # Generate new ticket
    new_data = {
        "Client_ID": client_id,
        "Account_ID": account_id,
        "Data_Source_Name": data_source_name,
        "Client_Name": client_name,
    }

    # Add campaign info if applicable
    if campaign_id and campaign_name:
        new_data["Campaign_ID"] = campaign_id
        new_data["Campaign_Name"] = campaign_name
    else:
        new_data["Campaign_ID"] = ""
        new_data["Campaign_Name"] = ""

    df_new = pd.DataFrame([new_data])
    
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
