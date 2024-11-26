import datetime
import pandas as pd
import streamlit as st

# Set Streamlit page config
st.set_page_config(page_title="The Reef", page_icon="🎫")

# App title
st.title("🎫 The Reef")

# Load data from CSV
df_path = 'the-reef/reefpaidmedia.csv'
try:
    df = pd.read_csv(df_path)
except FileNotFoundError:
    st.error(f"Could not find the file at `{df_path}`. Please check the path.")
    st.stop()

# Initialize session state
if "df" not in st.session_state:
    st.session_state.df = df

# Section to add a new ticket
st.header("Add an Account")
with st.form("add_ticket_form"):
    account_id = st.text_area("Account ID")
    
    # Populate client_id options dynamically
    client_id_options = st.session_state.df['client_id'].unique().tolist()
    client_id = st.selectbox("Client ID", client_id_options)
    
    data_source_name = st.selectbox(
        "Data Source", 
        ["Google Ads", "Microsoft Ads", "Facebook Ads", "El Toro", "Adelphic"]
    )
    submitted = st.form_submit_button("Submit")

if submitted:
    # Generate new ticket
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    df_new = pd.DataFrame(
        [{
            "Client ID": client_id,
            "Account ID": account_id,
            "Status": "Active",
            "Data Source": data_source_name,
            "Date Submitted": today,
        }]
    )
    st.write("Account submitted! Here are the account details:")
    st.dataframe(df_new, use_container_width=True, hide_index=True)

    # Update session state
    st.session_state.df = pd.concat([df_new, st.session_state.df], axis=0)

# Show and edit existing tickets
st.header("The Reef")
st.write(f"Number of tickets: `{len(st.session_state.df)}`")
st.info(
    "You can edit the tickets by double-clicking on a cell. Note how the plots below "
    "update automatically! You can also sort the table by clicking on the column headers.",
    icon="✍️"
)

# Display tickets with editable columns
edited_df = st.data_editor(
    st.session_state.df,
    use_container_width=True,
    hide_index=True,
)
