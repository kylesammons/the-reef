import datetime
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import streamlit as st

# Set Streamlit page config
st.set_page_config(page_title="The Reef", page_icon="🎫")

# App title
st.title("🎫 The Reef")


# Fetch data from BigQuery
df = 'reefpaidmedia.csv'


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
            "Status": "Active",
            "Priority": "Medium",
            "Data Source": data_source_name,
            "Date Submitted": today,
        }]
    )
    st.write("Ticket submitted! Here are the ticket details:")
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


# Display the DataFrame with editable columns
st.header("Edit Tickets")

# Configure AgGrid options
gb = GridOptionsBuilder.from_dataframe(st.session_state.df)
gb.configure_default_column(editable=True, resizable=True)
gb.configure_grid_options(enableRangeSelection=True, domLayout='autoHeight')

# Enable editing
grid_options = gb.build()

# Display the grid with AgGrid
response = AgGrid(
    st.session_state.df,
    gridOptions=grid_options,
    update_mode=GridUpdateMode.VALUE_CHANGED,
    fit_columns_on_grid_load=True,
    theme="streamlit",  # Use "streamlit" theme for better UI integration
)

# Update session state with edited data
if response['data'] is not None:
    st.session_state.df = pd.DataFrame(response['data'])

# Display a message or confirmation after editing
st.success("Data updated! Changes are reflected in real-time.")
