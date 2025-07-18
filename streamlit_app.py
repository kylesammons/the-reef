import os
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import time
import json

# Set Streamlit page config
st.set_page_config(page_title="The Reef", page_icon=":ocean:", layout="wide", initial_sidebar_state="expanded")

# BigQuery configuration
PROJECT_ID = "trimark-tdp"  # Replace with your actual project ID
DATASET_ID = "reference"     # Replace with your dataset name
TABLE_ID = "paidmedia_test"      # Replace with your table name

@st.cache_resource
def init_bigquery_client():
    """Initialize BigQuery client with service account credentials"""
    try:
        credentials = None
        
        # Method 1: Try Streamlit secrets (for deployment)
        try:
            if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
                credentials = service_account.Credentials.from_service_account_info(
                    st.secrets["gcp_service_account"]
                )
        except Exception as e:
            pass  # Continue to next method
        
        # Method 2: Try environment variable (recommended for local)
        if not credentials:
            try:
                credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
                if credentials_path and os.path.exists(credentials_path):
                    credentials = service_account.Credentials.from_service_account_file(credentials_path)
                    st.success(f"Using environment variable credentials: {credentials_path}")
            except Exception as e:
                pass  # Continue to next method
        
        # Method 3: Try hardcoded path (fallback for local development)
        if not credentials:
            try:
                hardcoded_path = '/Users/trimark/Desktop/Jupyter_Notebooks/trimark-tdp-87c89fbd0816.json'
                if os.path.exists(hardcoded_path):
                    credentials = service_account.Credentials.from_service_account_file(hardcoded_path)
            except Exception as e:
                pass  # Continue to next method
        
        # Method 4: Try default credentials (for Google Cloud environments)
        if not credentials:
            try:
                credentials, project = service_account.default()
                st.info("Using default Google Cloud credentials")
            except Exception as e:
                pass
        
        if not credentials:
            raise Exception("No valid credentials found. Please check your setup.")
        
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        
        # Test the connection
        try:
            # Simple query to test connection
            test_query = f"SELECT 1 as test_connection LIMIT 1"
            client.query(test_query).result()
        except Exception as e:
            st.warning(f"BigQuery connection test failed: {str(e)}")
        
        return client
        
    except Exception as e:
        st.error(f"Error initializing BigQuery client: {str(e)}")
        st.error("Please ensure you have set up authentication properly:")
        st.code("""
        For local development, set environment variable:
        export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"
        
        Or place your credentials file at:
        /Users/trimark/Desktop/Jupyter_Notebooks/trimark-tdp-87c89fbd0816.json
        
        For deployment, add your service account key to Streamlit secrets.
        """)
        return None

def load_data_from_bigquery():
    """Load data from BigQuery table"""
    client = init_bigquery_client()
    if not client:
        return pd.DataFrame(columns=["Client_ID", "Account_ID", "Data_Source_Name", "Client_Name", "Campaign_ID", "Campaign_Name"])
    
    try:
        query = f"""
        SELECT 
            Client_ID, 
            Account_ID, 
            Data_Source_Name, 
            Client_Name, 
            CAST(Campaign_ID AS STRING) as Campaign_ID,
            Campaign_Name
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        ORDER BY Client_Name, Data_Source_Name
        """
        
        df = client.query(query).to_dataframe()
        
        # Ensure all columns are strings and handle nulls
        df = df.fillna('')  # Replace NaN with empty strings
        # Convert Campaign_ID column specifically to handle nulls properly
        df['Campaign_ID'] = df['Campaign_ID'].astype(str).replace('None', '')
        df = df.astype(str)  # Convert all columns to string type
        
        return df
    except Exception as e:
        st.error(f"Error loading data from BigQuery: {str(e)}")
        return pd.DataFrame(columns=["Client_ID", "Account_ID", "Data_Source_Name", "Client_Name", "Campaign_ID", "Campaign_Name"])

def save_data_to_bigquery(df):
    """Save DataFrame to BigQuery table"""
    client = init_bigquery_client()
    if not client:
        return False
    
    try:
        # Create a copy of the dataframe to avoid modifying the original
        df_to_save = df.copy()
        
        # Convert Campaign_ID column: empty strings to None (NULL in BigQuery)
        df_to_save['Campaign_ID'] = df_to_save['Campaign_ID'].apply(
            lambda x: int(x) if x and x.isdigit() else None
        )
        
        # Configure the job to replace the table
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # This replaces the table
            autodetect=True,
        )
        
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        job = client.load_table_from_dataframe(df_to_save, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        
        return True
    except Exception as e:
        st.error(f"Error saving data to BigQuery: {str(e)}")
        return False

@st.dialog("Delete Confirmation")
def delete_confirmation_dialog(selected_count):
    """Show delete confirmation dialog"""
    st.warning(f"⚠️ **Are you sure you want to delete {selected_count} row(s)?**")
    st.write("This action cannot be undone.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Delete", key="modal_confirm_delete", type="primary", use_container_width=True):
            st.session_state.confirm_deletion = True
            st.rerun()
    
    with col2:
        if st.button("Cancel", key="modal_cancel_delete", use_container_width=True):
            st.session_state.show_delete_confirmation = False
            st.rerun()

# App title
st.title("The Reef")
st.info(
    "Add Accounts by entering required inputs in the sidebar. You can edit or delete the accounts by selecting Update in the table and following the prompts. "
    "Note how the action buttons appear when you make changes!",
    icon="✍️"
)

# Initialize session state
if "df" not in st.session_state:
    with st.spinner("Loading data from BigQuery..."):
        st.session_state.df = load_data_from_bigquery()

# Initialize original dataframe for comparison (to track changes)
if "original_df" not in st.session_state:
    st.session_state.original_df = st.session_state.df.copy()

# Initialize pending accounts dataframe
if "pending_accounts" not in st.session_state:
    st.session_state.pending_accounts = pd.DataFrame(columns=["Client_ID", "Account_ID", "Data_Source_Name", "Client_Name", "Campaign_ID", "Campaign_Name"])

# Initialize delete confirmation state
if "show_delete_confirmation" not in st.session_state:
    st.session_state.show_delete_confirmation = False

# Initialize deletion confirmation state
if "confirm_deletion" not in st.session_state:
    st.session_state.confirm_deletion = False

# Sidebar for inputs
with st.sidebar:
    st.image("Waves-Logo_Color.svg", width=200)
    st.markdown("<br>", unsafe_allow_html=True)
    st.header("Add an Account")
    
    # Client Name (required)
    client_name_options = st.session_state.df["Client_Name"].unique()
    if len(client_name_options) == 0:
        client_name_options = ["No clients available"]
    
    client_name = st.selectbox(
        "Client Name *", 
        client_name_options,
        help="Required field"
    )
    
    st.write("")  # Add spacing
    
    # Data Source (required)
    data_source_options = st.session_state.df["Data_Source_Name"].unique()
    if len(data_source_options) == 0:
        data_source_options = ["No data sources available"]
        
    data_source_name = st.selectbox(
        "Data Source *", 
        data_source_options,
        help="Required field"
    )
    
    st.write("")  # Add spacing
    
    # Account ID (required)
    account_id = st.text_input(
        "Account ID *", 
        help="Required field"
    )
    
    # Conditionally display Campaign ID for Window World + Facebook Ads
    campaign_id = None
    if "Window World" in client_name and data_source_name == "Facebook Ads":
        st.write("")  # Add spacing
        campaign_id = st.text_input(
            "Campaign ID *",
            help="Required field for Window World Facebook Ads"
        )
    
    st.write("")  # Add spacing before button
    st.write("")  # Extra spacing
    
    # Submit button
    submitted = st.button("Submit", use_container_width=True)
    
    # Form validation
    form_valid = True
    error_messages = []
    
    if submitted:
        if not client_name or client_name == "No clients available":
            form_valid = False
            error_messages.append("Client Name is required")
        
        if not data_source_name or data_source_name == "No data sources available":
            form_valid = False
            error_messages.append("Data Source is required")
            
        if not account_id:
            form_valid = False
            error_messages.append("Account ID is required")
            
        if "Window World" in client_name and data_source_name == "Facebook Ads" and not campaign_id:
            form_valid = False
            error_messages.append("Campaign ID is required for Window World Facebook Ads")
    
    # Display validation errors
    if submitted and not form_valid:
        for error in error_messages:
            st.error(error)

# Handle form submission - add to pending accounts
if submitted and form_valid:
    # Get Client_ID based on selected Client_Name
    client_ids = st.session_state.df[st.session_state.df["Client_Name"] == client_name]["Client_ID"].unique()
    client_id = client_ids[0] if len(client_ids) > 0 else None
    
    # Generate new account data
    new_account = pd.DataFrame([{
        "Client_ID": client_id,
        "Account_ID": account_id,
        "Data_Source_Name": data_source_name,
        "Client_Name": client_name,
        "Campaign_ID": campaign_id if campaign_id else "",
        "Campaign_Name": ""  # Empty for now, can be filled later
    }])
    
    # Add to pending accounts
    st.session_state.pending_accounts = pd.concat([st.session_state.pending_accounts, new_account], ignore_index=True)

# Initialize master access in session state
if "master_access" not in st.session_state:
    st.session_state.master_access = False

# Main content area with tabs (always show Master tab)
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Paid Media", "GA4", "Forms", "Marchex", "Search Console", "🔒 Master"])

with tab1:
    st.header("Paid Media")
    
    # Show pending accounts if any
    if not st.session_state.pending_accounts.empty:
        st.write("Account submitted! Here are the details:")
        
        # Select only the desired columns for display
        columns_to_display = ["Account_ID", "Data_Source_Name", "Client_ID", "Client_Name", "Campaign_ID", "Campaign_Name"]
        available_columns = [col for col in columns_to_display if col in st.session_state.pending_accounts.columns]
        
        # Display pending accounts as editable dataframe
        edited_pending = st.data_editor(
            st.session_state.pending_accounts[available_columns],
            use_container_width=True,
            hide_index=True,
            key="pending_accounts_editor"
        )
        
        # Update pending accounts with any edits
        for col in available_columns:
            st.session_state.pending_accounts[col] = edited_pending[col]
        
        # Buttons for pending accounts
        col1, col2, col3 = st.columns([1, 1, 4])
        
        with col1:
            if st.button("💾 Save All", key="save_all_accounts", type="primary"):
                # Create the final dataframe to save (original + pending)
                final_df = pd.concat([st.session_state.df, st.session_state.pending_accounts], ignore_index=True)
                
                with st.spinner("Saving all changes to BigQuery..."):
                    if save_data_to_bigquery(final_df):
                        # Update session state with the saved data
                        st.session_state.df = final_df.copy()
                        st.session_state.original_df = final_df.copy()
                        # Clear pending accounts since they're now saved
                        st.session_state.pending_accounts = pd.DataFrame(columns=["Client_ID", "Account_ID", "Data_Source_Name", "Client_Name", "Campaign_ID", "Campaign_Name"])
                        st.toast("All accounts saved successfully to BigQuery!", icon="✅")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("❌ Failed to save changes to BigQuery")
        
        with col2:
            if st.button("🗑️ Clear Pending", type="secondary"):
                st.session_state.pending_accounts = pd.DataFrame(columns=["Client_ID", "Account_ID", "Data_Source_Name", "Client_Name", "Campaign_ID", "Campaign_Name"])
                st.rerun()

    # Combined dataframe for display (original + pending + edits)
    if st.session_state.pending_accounts.empty:
        display_df = st.session_state.df.copy()
    else:
        display_df = pd.concat([st.session_state.df, st.session_state.pending_accounts], ignore_index=True)
    
    # Select only the desired columns for display and add checkbox column
    columns_to_display = ["Account_ID", "Data_Source_Name", "Client_ID", "Client_Name", "Campaign_ID", "Campaign_Name"]
    
    # Check if dataframe has the required columns
    available_columns = [col for col in columns_to_display if col in display_df.columns]
    
    if len(available_columns) > 0:
        # Create a placeholder for the status message above the data editor
        status_placeholder = st.empty()
        
        # Check if there are pending accounts
        has_pending = not st.session_state.pending_accounts.empty
        
        # Add checkbox column for update selection
        display_df_with_checkbox = display_df[available_columns].copy()
        display_df_with_checkbox["Update"] = False  # Add checkbox column
        
        # Display the data editor for all accounts (read-only)
        st.write(f"Paid Media Accounts: `{len(st.session_state.df)}` | Pending Accounts: `{len(st.session_state.pending_accounts)}`")
        
        # Create the data editor with only Update column editable
        edited_df = st.data_editor(
            display_df_with_checkbox,
            use_container_width=True,
            hide_index=True,
            key="all_accounts_editor",
            disabled=available_columns,  # Disable all columns except Update
            column_config={
                "Update": st.column_config.CheckboxColumn(
                    "Update",
                    help="Check to edit this row",
                    default=False,
                    width="small"
                )
            }
        )
        
        # Get selected rows for editing
        selected_rows = edited_df[edited_df["Update"] == True]
        selected_for_update = len(selected_rows) > 0
        
        # Show toast for row selection changes
        if selected_for_update:
            if "last_selected_count" not in st.session_state:
                st.session_state.last_selected_count = 0
            
            current_count = len(selected_rows)
            if current_count != st.session_state.last_selected_count:
                if current_count > 0:
                    st.toast(f"{current_count} row(s) selected for editing", icon="📝")
                st.session_state.last_selected_count = current_count
        else:
            if "last_selected_count" in st.session_state and st.session_state.last_selected_count > 0:
                st.session_state.last_selected_count = 0
        
        # Show status message
        if has_pending:
            status_placeholder.warning("⚠️ You have pending accounts.")
        else:
            status_placeholder.empty()
        
        # Show editable section for selected rows
        if selected_for_update:
            st.markdown("---")
            st.subheader("Edit Selected Rows")
            
            # Create editable dataframe from selected rows (without Update column)
            editable_rows = selected_rows.drop(columns=["Update"]).copy()
            
            # Display editable dataframe
            edited_selected = st.data_editor(
                editable_rows,
                use_container_width=True,
                hide_index=True,
                key="editable_selected_rows",
                num_rows="fixed"
            )
            
            # Action buttons for selected rows
            col1, col2, col3 = st.columns([1, 1, 4])
            
            with col1:
                if st.button("💾 Save All", key="save_selected_rows_btn", type="primary"):
                    # Update the main dataframe with edited values
                    selected_indices = edited_df[edited_df["Update"] == True].index
                    
                    # Update the display dataframe with edited values
                    for i, idx in enumerate(selected_indices):
                        for col in available_columns:
                            display_df.loc[idx, col] = edited_selected.iloc[i][col]
                    
                    # Determine which part is original vs pending
                    original_count = len(st.session_state.df)
                    
                    # Update session state
                    if len(display_df) > 0:
                        if original_count > 0:
                            st.session_state.df = display_df.iloc[:original_count].copy()
                        if len(display_df) > original_count:
                            st.session_state.pending_accounts = display_df.iloc[original_count:].copy()
                    
                    # Save to BigQuery
                    with st.spinner("Saving changes to BigQuery..."):
                        if save_data_to_bigquery(display_df):
                            st.session_state.df = display_df.copy()
                            st.session_state.original_df = display_df.copy()
                            st.session_state.pending_accounts = pd.DataFrame(columns=["Client_ID", "Account_ID", "Data_Source_Name", "Client_Name", "Campaign_ID", "Campaign_Name"])
                            st.toast("✅ Changes saved successfully to BigQuery!", icon="✅")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("❌ Failed to save changes to BigQuery")
            
            with col2:
                if st.button("🗑️ Delete", key="delete_selected_btn", type="secondary"):
                    # Show popup confirmation dialog
                    st.session_state.show_delete_confirmation = True
                    st.session_state.confirm_deletion = False
            
            # Show the delete confirmation popup if requested
            if st.session_state.get("show_delete_confirmation", False):
                selected_count = len(selected_rows)
                delete_confirmation_dialog(selected_count)
            
            # Handle confirmed deletion
            if st.session_state.get("confirm_deletion", False):
                # Get indices of selected rows
                selected_indices = edited_df[edited_df["Update"] == True].index
                
                # Remove selected rows
                remaining_rows = display_df.drop(selected_indices).reset_index(drop=True)
                
                # Update session state
                original_count = len(st.session_state.df)
                
                if len(remaining_rows) > 0:
                    if original_count > 0:
                        st.session_state.df = remaining_rows.iloc[:min(len(remaining_rows), original_count)].copy()
                    if len(remaining_rows) > original_count:
                        st.session_state.pending_accounts = remaining_rows.iloc[original_count:].copy()
                    else:
                        st.session_state.pending_accounts = pd.DataFrame(columns=["Client_ID", "Account_ID", "Data_Source_Name", "Client_Name", "Campaign_ID", "Campaign_Name"])
                else:
                    st.session_state.df = pd.DataFrame(columns=["Client_ID", "Account_ID", "Data_Source_Name", "Client_Name", "Campaign_ID", "Campaign_Name"])
                    st.session_state.pending_accounts = pd.DataFrame(columns=["Client_ID", "Account_ID", "Data_Source_Name", "Client_Name", "Campaign_ID", "Campaign_Name"])
                
                # Save to BigQuery
                final_df = pd.concat([st.session_state.df, st.session_state.pending_accounts], ignore_index=True)
                
                with st.spinner("Deleting rows from BigQuery..."):
                    if save_data_to_bigquery(final_df):
                        st.session_state.original_df = final_df.copy()
                        st.toast(f"🗑️ {len(selected_indices)} row(s) deleted successfully!", icon="🗑️")
                        # Reset confirmation states
                        st.session_state.show_delete_confirmation = False
                        st.session_state.confirm_deletion = False
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("❌ Failed to delete rows from BigQuery")
                        # Reset confirmation states
                        st.session_state.show_delete_confirmation = False
                        st.session_state.confirm_deletion = False
        
        # Action buttons for pending accounts (only show if no rows selected for update)
        elif has_pending:
            st.markdown("---")
            col1, col2, col3 = st.columns([1, 1, 4])
            
            with col1:
                if st.button("💾 Save All", key="save_pending_accounts_btn", type="primary"):
                    # Save pending accounts
                    final_df = pd.concat([st.session_state.df, st.session_state.pending_accounts], ignore_index=True)
                    
                    with st.spinner("Saving all changes to BigQuery..."):
                        if save_data_to_bigquery(final_df):
                            st.session_state.df = final_df.copy()
                            st.session_state.original_df = final_df.copy()
                            st.session_state.pending_accounts = pd.DataFrame(columns=["Client_ID", "Account_ID", "Data_Source_Name", "Client_Name", "Campaign_ID", "Campaign_Name"])
                            st.toast("✅ All pending accounts saved successfully!", icon="✅")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("❌ Failed to save changes to BigQuery")
            
            with col2:
                if st.button("🗑️ Clear Pending", key="clear_pending_accounts_btn", type="secondary"):
                    st.session_state.pending_accounts = pd.DataFrame(columns=["Client_ID", "Account_ID", "Data_Source_Name", "Client_Name", "Campaign_ID", "Campaign_Name"])
                    st.toast("🧹 Pending accounts cleared", icon="🧹")
                    st.rerun()
    else:
        st.warning("No data available to display.")
        
with tab2:
    st.header("GA4")
    st.info("GA4 content coming soon...")

with tab3:
    st.header("Forms")
    st.info("Forms content coming soon...")

with tab4:
    st.header("Marchex")
    st.info("Marchex content coming soon...")

with tab5:
    st.header("Search Console")
    st.info("Search Console content coming soon...")

# Master tab (always visible, but content depends on authentication)
with tab6:
    if not st.session_state.master_access:
        st.header("Master Access")
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            master_password = st.text_input("Master Password:", type="password", key="master_pwd")
        
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button("Unlock", type="primary"):
                # Change this password to whatever you want
                if master_password == "reef2025":
                    st.session_state.master_access = True
                    st.success("Access granted!")
                    st.rerun()
                else:
                    st.error("Invalid password")
    else:
        # Show master control panel
        st.header("🔒 Master Control Panel")
        
        # Master controls
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Database Management")
            
            # Download current data
            if st.button("📥 Download CSV"):
                csv_data = st.session_state.df.to_csv(index=False)
                st.download_button(
                    label="Download reefpaidmedia.csv",
                    data=csv_data,
                    file_name="reefpaidmedia.csv",
                    mime="text/csv"
                )
            
            # Refresh data from BigQuery
            if st.button("🔄 Refresh from BigQuery"):
                with st.spinner("Refreshing data..."):
                    st.session_state.df = load_data_from_bigquery()
                    st.session_state.original_df = st.session_state.df.copy()
                    # Clear pending accounts on refresh
                    st.session_state.pending_accounts = pd.DataFrame(columns=["Client_ID", "Account_ID", "Data_Source_Name", "Client_Name", "Campaign_ID", "Campaign_Name"])
                    st.success("Data refreshed!")
                    st.rerun()
            
            # Clear all data (with confirmation)
            if st.button("🗑️ Clear All Data", type="secondary"):
                if st.button("⚠️ Confirm Delete All", type="secondary"):
                    empty_df = pd.DataFrame(columns=["Client_ID", "Account_ID", "Data_Source_Name", "Client_Name", "Campaign_ID", "Campaign_Name"])
                    with st.spinner("Clearing all data..."):
                        if save_data_to_bigquery(empty_df):
                            st.session_state.df = empty_df
                            st.session_state.original_df = empty_df.copy()
                            st.session_state.pending_accounts = empty_df.copy()
                            st.success("All data cleared!")
                            st.rerun()
                        else:
                            st.error("Failed to clear data")
        
        with col2:
            st.subheader("System Info")
            st.write(f"**Total Records in BigQuery:** {len(st.session_state.df)}")
            st.write(f"**Pending Accounts:** {len(st.session_state.pending_accounts)}")
            st.write(f"**Unique Clients:** {st.session_state.df['Client_Name'].nunique()}")
            st.write(f"**Data Sources:** {', '.join(st.session_state.df['Data_Source_Name'].unique())}")
            
            # Raw data view
            if st.checkbox("Show Raw BigQuery Data"):
                st.dataframe(st.session_state.df, use_container_width=True)
            
            if st.checkbox("Show Pending Accounts"):
                st.dataframe(st.session_state.pending_accounts, use_container_width=True)
        
        # Logout from master
        st.write("---")
        if st.button("🚪 Logout from Master", type="secondary"):
            st.session_state.master_access = False
            st.rerun()
