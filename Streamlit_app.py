import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from datetime import datetime, date
import json

# Constants
ENTITY_LOOKUP_TABLE = "ENTITY_LOOKUP"
DIM_ASOFDATE_TABLE = "DIM_ASOFDATE"
DEFAULT_MODULE_ID = 9999

# Page configuration
st.set_page_config(
    page_title="Editable Data Override App",
    page_icon="✨",
    layout="wide"
)

# Custom CSS for a better look and feel
st.markdown("""
    <style>
        .module-box {
            background-color: #2196F3; /* Blue */
            color: white;
            padding: 15px;
            border-radius: 10px;
            font-size: 20px;
            font-weight: bold;
            text-align: center;
            margin-bottom: 20px;
        }
        .stButton>button {
            background-color: #2196F3; /* Blue */
            color: white;
            padding: 10px 20px;
            border-radius: 20px;
            font-size: 16px;
            border: none;
            cursor: pointer;
        }
        .stButton>button:hover {
            background-color: #1976D2;
            color: white;
        }
        .warning-box {
            background-color: #FFD54F; /* Amber */
            color: #D84315;
            padding: 10px;
            border-radius: 5px;
            margin-bottom: 20px;
            text-align: center;
        }
        .table-header {
            font-weight: bold;
            background-color: #E3F2FD; /* Light Blue */
            border-top: 2px solid #90CAF9;
            border-bottom: 2px solid #90CAF9;
        }
        .ag-theme-streamlit {
            --ag-header-background-color: #2196F3;
            --ag-header-foreground-color: white;
            --ag-data-color: black;
            --ag-row-hover-color: #BBDEFB;
            --ag-odd-row-background-color: #E3F2FD;
            --ag-even-row-background-color: #FFFFFF;
            --ag-border-color: #90CAF9;
        }
        .center-checkbox {
            display: flex;
            align-items: center;
            justify-content: center;
            height: 100%;
        }
        .element-container {
            font-family: 'Arial', sans-serif;
            font-size: 14px;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
        }
        body {
            background-color: #F9F9F9; /* Light Grey */
            font-family: 'Arial', sans-serif;
        }
    </style>
""", unsafe_allow_html=True)

# Title with a splash of color
st.markdown("<h1 style='text-align: center; color: #2196F3;'>Override Dashboard</h1>", unsafe_allow_html=True)

# Initialize session state
if 'current_step' not in st.session_state:
    st.session_state.current_step = "select"
if 'selected_rows' not in st.session_state:
    st.session_state.selected_rows = []
if 'edited_rows' not in st.session_state:
    st.session_state.edited_rows = []
if 'validation_warnings' not in st.session_state:
    st.session_state.validation_warnings = []
if 'view_data' not in st.session_state:
    st.session_state.view_data = None
if 'override_data' not in st.session_state:
    st.session_state.override_data = None
if 'last_update_time' not in st.session_state:
    st.session_state.last_update_time = None
if 'selected_module_id' not in st.session_state:
    st.session_state.selected_module_id = DEFAULT_MODULE_ID
if 'config' not in st.session_state:
    st.session_state.config = None
if 'initial_load' not in st.session_state:
    st.session_state.initial_load = True

# Snowflake session
try:
    session = get_active_session()
    if session:
        st.success("✅ Connected to Snowflake")
    else:
        st.error("❌ No active Snowflake session.")
        st.stop()
except Exception as e:
    st.error(f"❌ Snowflake connection error: {e}")
    st.stop()

# Fetch data from Snowflake table
def fetch_data(table_name):
    try:
        df = session.sql(f"SELECT * FROM {table_name}").to_pandas()
        df.columns = [col.strip().upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"❌ Error reading {table_name}: {e}")
        return pd.DataFrame()

# ENTITY_LOOKUP
entity_lookup_df = fetch_data(ENTITY_LOOKUP_TABLE)
if entity_lookup_df.empty:
    st.error(f"No data found in {ENTITY_LOOKUP_TABLE}")
    st.stop()

entity_lookup_df['ENT_ID'] = entity_lookup_df['ENT_ID'].astype(int)
# Module selection (improved logic)
# Module selection (robust logic)
module_ids = entity_lookup_df['ENT_ID'].tolist()
query_params = st.query_params

# Support both 'streamlit-module_id' and 'module_id' for flexibility
module_id_str = query_params.get("streamlit-module_id") or query_params.get("module_id")

if module_id_str is not None:
    try:
        module_id = int(module_id_str)
        if module_id in module_ids:
            selected_module_id = module_id
            st.session_state.selected_module_id = selected_module_id  # Persist it
        else:
            st.error(f"Module ID {module_id} from URL not found in ENTITY_LOOKUP.")
            st.stop()
    except ValueError:
        st.error("Invalid module_id parameter in URL. Must be an integer.")
        st.stop()
else:
    # If no module_id in URL, show dropdown for all modules
    # Ensure the module IDs are sorted in ascending order
    module_ids_sorted = sorted(module_ids)
    selected_module_id = st.selectbox(
        "Select Module",
        module_ids_sorted,
        index=module_ids_sorted.index(st.session_state.selected_module_id)
            if st.session_state.selected_module_id in module_ids_sorted else 0,
        key="module_select"
    )
    
    # Update session state if changed
    if selected_module_id != st.session_state.selected_module_id:
        st.session_state.selected_module_id = selected_module_id
        st.session_state.initial_load = True  # Ensure config/data reloads


# Always use st.session_state.selected_module_id in the rest of your code

# Load config based on selected_module_id
def load_config(module_id):
    config_row = entity_lookup_df[entity_lookup_df['ENT_ID'] == module_id]
    if config_row.empty:
        st.error(f"No config found for ENT_ID = {module_id}")
        return None
    try:
        config = json.loads(config_row['KEY'].iloc[0]) if isinstance(config_row['KEY'].iloc[0], str) else config_row['KEY'].iloc[0]
        return config
    except Exception as e:
        st.error(f"❌ Invalid configuration format: {e}")
        return None

# Function to load view data
def load_view_data():
    if st.session_state.config:
        view_name = st.session_state.config.get("view")
        try:
            df_view = session.sql(f"SELECT * FROM {view_name}").to_pandas()
            st.session_state.view_data = df_view
        except Exception as e:
            st.error(f"❌ Error loading view data: {e}")
            st.session_state.view_data = None
    else:
        st.warning("Configuration not loaded.")


# Function to load override data
def load_override_data():
    if st.session_state.config:
        override_table = st.session_state.config.get("override_table")
        try:
            df_override = fetch_data(override_table)
            st.session_state.override_data = df_override
        except Exception as e:
            st.error(f"❌ Error loading override data: {e}")
            st.session_state.override_data = None
    else:
        st.warning("Configuration not loaded.")

# Load config, view_data, and override_data only if selected_module_id changes
if st.session_state.initial_load or st.session_state.selected_module_id != selected_module_id:
    st.session_state.initial_load = False  # Set initial_load to False after the initial loading
    st.session_state.config = load_config(selected_module_id)
    if st.session_state.config:
        load_view_data()
        load_override_data()
    else:
        st.session_state.view_data = None
        st.session_state.override_data = None

    st.session_state.selected_module_id = selected_module_id # Update selected_module_id

# Helper functions
def calculate_percent_change(old_value, new_value):
    if old_value == 0:
        return float('inf') if new_value > 0 else 0
    return ((new_value - old_value) / abs(old_value)) * 100

# Function to validate changes before submission
def validate_changes():
    warnings = []
    if not st.session_state.config:
        st.warning("Please select a module to load the configuration.")
        return

    editable_cols = st.session_state.config.get("editable_cols", [])
    if not editable_cols:
        st.warning("No editable columns defined in the configuration.")
        return

    for row in st.session_state.edited_rows:
        # Dynamically determine the column to compare based on editable_cols
        editable_col = editable_cols[0]  # Assuming the first editable column is the one to compare
        old_val = row.get(f'OLD_{editable_col.upper()}')
        new_val = row.get(f'NEW_{editable_col.upper()}')

        if pd.notna(old_val) and pd.notna(new_val) and old_val != new_val:
            percent_change = calculate_percent_change(old_val, new_val)
            row['PERCENT_CHANGE'] = percent_change  # Store percentage change for display
            if percent_change > 10:
                warnings.append(f"Warning: Your value change is increased more than 10% ({percent_change:.2f}%), do you want to proceed with this")
            elif percent_change < -10:
                warnings.append(f"Warning: Your value change is decreased more than 10% ({percent_change:.2f}%), do you want to proceed with this")
            if pd.notna(row.get('EFFECTIVE_START_DATE')) and pd.notna(row.get('ASOFDATE')):
                if row.get('EFFECTIVE_START_DATE') < row.get('ASOFDATE'):
                    warnings.append("Warning: Your effective start date is less than asofdate")

    st.session_state.validation_warnings = warnings
    st.session_state.current_step = "validate"

# Enhanced function to insert overrides with as_of_date fix and refresh view
def insert_overrides_enhanced():
    if not st.session_state.edited_rows:
        st.info("No changes to submit.")
        return

    dim_asof_df = fetch_data(DIM_ASOFDATE_TABLE)
    if dim_asof_df.empty or 'AS_OF_DATE' not in dim_asof_df.columns:
        st.error("❌ Failed to retrieve AS_OF_DATE.")
        return

    if not st.session_state.config:
        st.error("Please select a module first.")
        return

    source_table = st.session_state.config.get("table_names")
    if not source_table:
        st.error("Source table not defined in config.")
        return

    override_table = st.session_state.config.get("override_table")
    if not override_table:
        st.error("Override table not defined in config.")
        return

    key_cols = st.session_state.config.get("key_cols", [])
    editable_cols = st.session_state.config.get("editable_cols", [])

    source_df = fetch_data(source_table)
    if source_df.empty:
        st.error(f"❌ Failed to retrieve data from {source_table}.")
        return

    for i, row in enumerate(st.session_state.edited_rows):
        try:
            row_keys = {k.upper(): k for k in row.keys()}
            key_data = {}
            for k in key_cols:
                actual_key = row_keys.get(k.upper())
                if actual_key:
                    key_data[k.lower()] = str(row[actual_key])
                else:
                    key_data[k.lower()] = ""

            # Add as_of_date explicitly from the row or fallback to global as_of_date
            as_of_date_value = row.get('ASOFDATE') or row.get('AS_OF_DATE')

            if as_of_date_value:
                if isinstance(as_of_date_value, (datetime, date)):
                    as_of_date_value = as_of_date_value.strftime('%Y-%m-%d')
                elif not isinstance(as_of_date_value, str):
                    as_of_date_value = str(as_of_date_value)
                key_data['as_of_date'] = as_of_date_value
            else:
                st.error("AS_OF_DATE is missing or invalid.")
                continue

            # Use the first editable column to determine which values to use
            if not editable_cols:
                st.error("❌ No editable columns defined in the configuration.")
                return

            editable_col = editable_cols[0]
            old_val = row.get(f'OLD_{editable_col.upper()}')
            new_val = row.get(f'NEW_{editable_col.upper()}')

            old_data = {editable_col.lower(): old_val if pd.notna(old_val) else None}
            new_data = {editable_col.lower(): new_val if pd.notna(new_val) else None}

            # Calculate % Change
            percent_change = calculate_percent_change(old_val, new_val)
            percent_change_rounded = round(percent_change, 2)

            # Get comments
            comments = row.get('COMMENTS', '')  # Default to empty string if no comment
            eff_start_date = row.get('EFFECTIVE_START_DATE')
            eff_end_date = row.get('EFFECTIVE_END_DATE')

            # Validate eff_start_date and eff_end_date
            if not (eff_start_date and eff_end_date):
                st.error(f"Effective start and end dates are required for row {i+1}")
                continue

            # Convert eff_start_date and eff_end_date to strings if they are datetime or date objects
            if isinstance(eff_start_date, (datetime, date)):
                eff_start_date = eff_start_date.strftime('%Y-%m-%d')
            if isinstance(eff_end_date, (datetime, date)):
                eff_end_date = eff_end_date.strftime('%Y-%m-%d')

            key_json = json.dumps(key_data, indent=4, default=str)
            old_data_json = json.dumps(old_data, indent=4, default=str)
            new_data_json = json.dumps(new_data, indent=4, default=str)

            insert_sql = f"""
                INSERT INTO {override_table}
                (MODULE_ID, TABLE_NAME, KEY_VALUE, AS_OF_DATE, OLD_DATA, NEW_DATA, EFF_START_DATE, EFF_END_DATE, COMMENTS, "% CHANGE")
                SELECT
                    {selected_module_id},
                    '{source_table}',
                    PARSE_JSON('{key_json}'),
                    DATE('{as_of_date_value}'),
                    PARSE_JSON('{old_data_json}'),
                    PARSE_JSON('{new_data_json}'),
                    TO_DATE('{eff_start_date}'),
                    TO_DATE('{eff_end_date}'),
                    '{comments}',
                    {percent_change_rounded}
            """
            session.sql(insert_sql).collect()

        except Exception as e:
            st.error(f"Insertion failed for record {i + 1}: {e}\nSQL: {insert_sql}")
            continue

    st.success("✅ Override(s) submitted successfully.")
    st.session_state.last_update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Refresh view and override data after submission
    load_view_data()
    load_override_data()

# Module name display
if st.session_state.config:
    module_name = st.session_state.config.get("module_name", f"Module {st.session_state.selected_module_id}")
    st.markdown(f"<div class='module-box'>{module_name}</div>", unsafe_allow_html=True)
else:
    st.warning("Select a module to load the configuration.")



# Add Pagination
def paginate(df, page_size=15):
    total_records = len(df)
    total_pages = (total_records - 1) // page_size + 1
    
    # Initialize page number in session state if not present
    if 'page_num' not in st.session_state:
        st.session_state.page_num = 1
    
    start_idx = (st.session_state.page_num - 1) * page_size  # Starting index for the current page
    end_idx = start_idx + page_size  # Ending index for the current page
    paged_df = df.iloc[start_idx:end_idx]  # Slice the DataFrame to get current page's data

    return paged_df, total_pages


# Function to reset to source data (abort action)
def reset_to_source_data():
    st.session_state.current_step = "select"  # Go back to selecting records
    st.session_state.page_num = 1  # Reset page number to 1
    st.session_state.selected_rows = []  # Clear any selected rows
    st.rerun()  # Rerun the app to reflect the changes

# Tabs for source data and overrides
tab1, tab2 = st.tabs(["Source Data", "Source Data Overrides"])

with tab1:
    if st.session_state.config:
        view_name = st.session_state.config.get("view")
        st.subheader(f"Source Data from {view_name}")

        if st.session_state.view_data is not None:
            df_view = st.session_state.view_data.copy()
        else:
            st.warning("View data is not available. Please check the connection and view configuration.")
            df_view = pd.DataFrame()

        if df_view.empty:
            st.warning("No data found in view.")
            st.stop()

        # Data Filtering
        if st.session_state.current_step == "select":
            filter_container = st.container()
            with filter_container:
                filter_cols = st.columns(len(df_view.columns))
                filters = {}
                for i, column in enumerate(df_view.columns):
                    unique_vals = df_view[column].unique().tolist()
                    selected_filter = filter_cols[i].selectbox(f" {column}", options=["All"] + unique_vals, key=f"filter_{column}")
                    if selected_filter != "All":
                        filters[column] = selected_filter

                # Apply Filters
                filtered_df_view = df_view.copy()
                for column, value in filters.items():
                    filtered_df_view = filtered_df_view[filtered_df_view[column] == value]
        else:
            filtered_df_view = df_view.copy()

        # Paginate the filtered data
        paged_df, total_pages = paginate(filtered_df_view)

        # Display filtered and paginated data
        if st.session_state.current_step == "select":
            # Create a list to store the selected rows
            selected_rows = []

            # Display column headers outside of the loop, ensuring they are always visible
            cols = st.columns([0.1] + [0.4] * len(paged_df.columns))  # Adjust column width as needed
            for i, column in enumerate(paged_df.columns):
                with cols[i + 1]:
                    st.markdown(f"**{column}**", unsafe_allow_html=True)  # Display column names in bold

            # Table display with select checkboxes
            for i, row in paged_df.iterrows():
                cols = st.columns([0.1] + [0.4] * len(paged_df.columns))  # Adjust column width as needed

                # Add checkbox in the first column
                is_selected = cols[0].checkbox("", key=f"select_{i}")

                # Display the row data
                for j, column in enumerate(paged_df.columns):
                    with cols[j + 1]:
                        st.write(str(row[column]))  # Display the row data as is

                # If the row is selected, add it to the selected_rows list
                if is_selected:
                    selected_rows.append(row.to_dict())

             # Pagination Controls (Previous and Next buttons)
            col1, col2, col3 = st.columns([1, 2, 1])  # Adjust column widths for buttons
            with col1:
                if st.button("Previous") and st.session_state.page_num > 1:
                    st.session_state.page_num -= 1  # Decrease page number
            
            with col2:
                     st.markdown(f"Page {st.session_state.page_num} of {total_pages}") 
            with col3:
                if st.button("Next") and st.session_state.page_num < total_pages:
                    st.session_state.page_num += 1  # Increase page number

            st.markdown("""
                <marquee behavior="alternate" direction="left" scrollamount="6" style="color:#2196F3; font-weight:bold; font-size:16px; margin-top: 20px;">
                    📌 Select the records which need to be modified or edited, then click 'Continue' to proceed.
                </marquee>
            """, unsafe_allow_html=True)

            # Continue button
            if st.button("Continue", key="continue_select"):
                if not selected_rows:
                    st.warning("Please select at least one row to continue.")
                else:
                    st.session_state.selected_rows = selected_rows
                    st.session_state.current_step = "edit"
                    st.rerun()

           

        elif st.session_state.current_step == "edit":
            st.subheader("Edit Selected Rows")
            edited_rows = []

            # Create a DataFrame from the selected rows in session state
            selected_df = pd.DataFrame(st.session_state.selected_rows)

            # Make sure the DataFrame is not empty
            if selected_df.empty:
                st.warning("No rows selected for editing.")
                st.stop()

            # Use the first editable column if available; otherwise, default to 'MARKET_VALUE'
            editable_cols = st.session_state.config.get("editable_cols", [])
            data_column = editable_cols[0].upper() if editable_cols else 'MARKET_VALUE'

            # Display column headers
            num_fixed_cols = 5  # Number of fixed editable columns (New Value, Start Date, End Date, Comments, % Change)
            cols = st.columns([0.1] + [0.2] * (len(selected_df.columns) + num_fixed_cols))
            for i, column in enumerate(selected_df.columns):
                with cols[i + 1]:
                    st.markdown(f"**{column}**", unsafe_allow_html=True)

            # Add headers for the new editable columns
            with cols[len(selected_df.columns) + 1]:
                st.markdown(f"**NEW_{data_column}**", unsafe_allow_html=True)
            with cols[len(selected_df.columns) + 2]:
                st.markdown(f"**EFFECTIVE_START_DATE**", unsafe_allow_html=True)
            with cols[len(selected_df.columns) + 3]:
                st.markdown(f"**EFFECTIVE_END_DATE**", unsafe_allow_html=True)
            with cols[len(selected_df.columns) + 4]:
                st.markdown(f"**COMMENTS**", unsafe_allow_html=True)
            with cols[len(selected_df.columns) + 5]:
                st.markdown(f"**% CHANGE**", unsafe_allow_html=True)

            # Display the table rows
            for i, row in selected_df.iterrows():
                cols = st.columns([0.1] + [0.2] * (len(selected_df.columns) + num_fixed_cols))

                # Display the original row data
                for j, column in enumerate(selected_df.columns):
                    with cols[j + 1]:
                        st.write(str(row[column]))

                # Input fields for the editable columns
                original_value = row.get(data_column, 0.0)
                new_value = cols[len(selected_df.columns) + 1].number_input("", value=original_value, key=f"new_{data_column}_{i}")
                effective_start_date = cols[len(selected_df.columns) + 2].date_input("", value=(row.get('EFFECTIVE_START_DATE') or datetime.now().date()), key=f"effective_start_date_{i}")
                effective_end_date = cols[len(selected_df.columns) + 3].date_input("", value=(row.get('EFFECTIVE_END_DATE') or datetime.now().date()), key=f"effective_end_date_{i}")
                comments = cols[len(selected_df.columns) + 4].text_input("", value=row.get('COMMENTS', ""), key=f"comments_{i}")

                # Calculate and display % Change
                percent_change = calculate_percent_change(original_value, new_value)
                cols[len(selected_df.columns) + 5].text_input("", value=f"{percent_change:.2f}%", disabled=True, key=f"percent_change_{i}")

                # Store the edited row data, prefixing the editable column
                edited_row = {**row.to_dict()}
                edited_row[f'OLD_{data_column}'] = original_value
                edited_row[f'NEW_{data_column}'] = new_value
                edited_row['EFFECTIVE_START_DATE'] = effective_start_date
                edited_row['EFFECTIVE_END_DATE'] = effective_end_date
                edited_row['COMMENTS'] = comments
                edited_row['PERCENT_CHANGE'] = percent_change
                edited_rows.append(edited_row)

            st.session_state.edited_rows = edited_rows  # Store the edited rows in session state

            # Validate button
            if st.button("Validate Changes", key="validate_changes"):
                validate_changes()
                if st.session_state.validation_warnings:
                    st.rerun()
             # Abort button to go back to Source Data
            if st.button("Abort", key="abort_select"):
                reset_to_source_data()  # This will go back to the Source Data step

        elif st.session_state.current_step == "validate":
            st.subheader("Validation")

            # Display validation messages
            if st.session_state.validation_warnings:
                for warning in st.session_state.validation_warnings:
                    st.warning(warning)

            # Display edited rows in a table
            if st.session_state.edited_rows:
                edited_df = pd.DataFrame(st.session_state.edited_rows)
                st.dataframe(edited_df) 
            else:
                st.info("No changes to validate.")
                st.stop()

            # Submit button
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Back to Edit", key="back_to_edit"):
                    st.session_state.current_step = "edit"
                    st.rerun()
            with col2:
                if st.button("Submit Overrides", key="submit_overrides"):
                    insert_overrides_enhanced()
                    st.session_state.current_step = "select"  # Reset to select after submission
                    st.rerun()
             # Abort button to go back to Source Data
            if st.button("Abort", key="abort_select"):
                reset_to_source_data()  # This will go back to the Source Data step
    else:
        st.warning("Select a module to view source data.")
    

with tab2:
    if st.session_state.config:
        override_table = st.session_state.config.get("override_table")
        st.subheader(f"Source Data Overrides from {override_table}")

        if st.session_state.override_data is not None:
            df_override = st.session_state.override_data.copy()
        else:
            st.warning("Override data is not available. Please check the connection and table configuration.")
            df_override = pd.DataFrame()

        if not df_override.empty:
            st.dataframe(df_override)
            if st.session_state.last_update_time:
                st.success(f"Last updated on: {st.session_state.last_update_time}")
        else:
            st.info("No override data found.")
    else:
        st.warning("Select a module to view override data.")

    	
# Footer with last update time
st.markdown("---")
st.caption(f"🕒 Last Updated: {st.session_state.last_update_time}")

# # Button to refresh data manually
# if st.button("Refresh Data"):
#     load_view_data()
#     load_override_data()
#     st.success("Data refreshed.")



#https://app.snowflake.com/elhhkec/cob40986/#/streamlit-apps/STREAMLIT_NEW.DAC.ZSD0_L9GLMLHO_05/edit?ref=snowsight_shared&streamlit-module_id=2
#https://app.snowflake.com/elhhkec/cob40986/#/streamlit-apps/STREAMLIT_NEW.DAC.ZSD0_L9GLMLHO_05/edit?ref=snowsight_shared&streamlit-module_id=1
