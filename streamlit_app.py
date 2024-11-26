import datetime
import pandas as pd
import streamlit as st

# Set Streamlit page config
st.set_page_config(page_title="The Reef", page_icon="🎫")

# App title
st.title("🎫 The Reef")

# Load the original DataFrame
csv_path = '/Users/Trimark/Desktop/Jupyter_Notebooks/reefpaidmedia.csv'
original_df = pd.read_csv(csv_path)

# Initialize session state
if "df" not in st.session_state:
    # Copy the original DataFrame to session state
    st.session_state.df = original_df.copy()

# Section to add a new ticket
st.header("Add an Account")
with st.form("add_ticket_form"):
    account_id = st.text_area("Account ID")
    client_id = st.text_area("Client ID")
    data_source_name = st.selectbox("Data Source", ["Google Ads", "Microsoft Ads", "Facebook Ads"])
    submitted = st.form_submit_button("Submit")

if submitted:
    # Generate a new ticket
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
    st.write("Ticket submitted! Here are the ticket details:")
    st.dataframe(df_new, use_container_width=True, hide_index=True)

    # Update session state DataFrame
    st.session_state.df = pd.concat([df_new, st.session_state.df], axis=0, ignore_index=True)

# Show and edit existing tickets
st.header("The Reef")
st.write(f"Number of tickets: `{len(st.session_state.df)}`")
st.info(
    "You can edit the tickets by double-clicking on a cell. Note how the plots below "
    "update automatically! You can also sort the table by clicking on the column headers.",
    icon="✍️"
)

# Display the session DataFrame with editable columns
edited_df = st.data_editor(
    st.session_state.df,
    use_container_width=True,
    hide_index=True,
)

# Optionally, update session state with edits
st.session_state.df = edited_df
