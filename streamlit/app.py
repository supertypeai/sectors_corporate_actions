import streamlit as st
import os
from supabase import create_client, Client
from dotenv import load_dotenv
import time
import pandas as pd
import json
from datetime import datetime


# Load environment variables from .env
load_dotenv()

# --- DB CONNECTION ---
URL = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_KEY")

# Initialize Supabase Client
supabase: Client = create_client(URL, KEY)

st.set_page_config(page_title="Corporate Action Entry", layout="wide")

# --- NAVIGATION ---
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to:", ["IDX Right Issue", "IDX Buyback", "IDX Reverse Stock Split"])

# --- SHARED FUNCTIONS ---
def upsert_to_supabase(table_name, payload):
    try:
        # Supabase .upsert handles insert or update based on Primary Key
        response = supabase.table(table_name).upsert(payload).execute()
        st.success(f"✅ Success! Data synced to {table_name}.")
    except Exception as e:
        st.error(f"❌ Database Error: {e}")

# --- PAGE: IDX RIGHT ---
if page == "IDX Right Issue":
    st.header("Upsert: IDX Right Issue")
    
    with st.form("idx_right_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            symbol = st.text_input("Symbol", placeholder="e.g. BBCA.JK")
            recording_date = st.date_input("Recording Date")
            cum_date = st.date_input("Cum Date")
            ex_date = st.date_input("Ex Date")
            # Set price and factor default to 0.0 but we will check it later
            price = st.number_input("Price", min_value=0.0, step=1.0)
            factor = st.number_input("Factor", min_value=0.0, step=0.0001)

        with col2:
            old_ratio = st.number_input("Old Ratio", min_value=0.0, step=0.01)
            new_ratio = st.number_input("New Ratio", min_value=0.0, step=0.01)
            tp_start = st.date_input("Trading Period Start")
            tp_end = st.date_input("Trading Period End")
            sub_date = st.date_input("Subscription Date")

        submitted = st.form_submit_button("Upsert Data")
        
        if submitted:
            # 1. Check if required fields are filled
            # Check if symbol is empty OR if ratios/prices are still zero
            if not symbol or old_ratio == 0 or new_ratio == 0 or price == 0:
                st.warning("⚠️ All fields are required. Please ensure Symbol is text and numeric values are greater than 0.")
            else:
                if ex_date < cum_date:
                    st.warning("⚠️ Ex Date cannot be earlier than Cum Date.")
                else:
                    data = {
                        "symbol": symbol,
                        "recording_date": recording_date.strftime("%Y-%m-%d"),
                        "old_ratio": old_ratio,
                        "new_ratio": new_ratio,
                        "price": price,
                        "factor": factor,
                        "cum_date": cum_date.strftime("%Y-%m-%d"),
                        "ex_date": ex_date.strftime("%Y-%m-%d"),
                        "trading_period_start": tp_start.strftime("%Y-%m-%d"),
                        "trading_period_end": tp_end.strftime("%Y-%m-%d"),
                        "subscription_date": sub_date.strftime("%Y-%m-%d"),
                        "updated_on": time.strftime("%Y-%m-%d %H:%M:%S")
                    }
                    upsert_to_supabase("idx_right_issue", data)

                    st.success(f"🟢🟢 Successfully added right issue for {symbol}! 🟢🟢")
                    st.cache_data.clear()
                    time.sleep(2)
                    st.rerun()

elif page == "IDX Reverse Stock Split":
    st.header("Upsert: IDX Reverse Stock Split")
    
    with st.form("idx_reverse_stock_split_form",clear_on_submit=True):
        symbol = st.text_input("Symbol", placeholder="e.g. BBCA.JK")
        recording_date = st.date_input("Recording Date")
        cum_date = st.date_input("Cum Date")
        ex_date = st.date_input("Ex Date")
        split_ratio = st.number_input("Split Ratio", min_value=0.0, step=0.01)

        submitted = st.form_submit_button("Upsert Data")

        if submitted:
            if not symbol or split_ratio == 0:
                st.warning("⚠️ All fields are required. Please ensure Symbol is text and numeric values are greater than 0.")
            else:
                if ex_date < cum_date:
                    st.warning("⚠️ Ex Date cannot be earlier than Cum Date.")
                else:
                    data = {
                        "symbol": symbol,
                        "split_ratio": split_ratio,
                        "recording_date": recording_date.strftime("%Y-%m-%d"),
                        "cum_date": cum_date.strftime("%Y-%m-%d"),
                        "date": ex_date.strftime("%Y-%m-%d"),
                        "updated_on": time.strftime("%Y-%m-%d %H:%M:%S")
                    }
                    upsert_to_supabase("idx_stock_split", data)
                    
                    st.success(f"🟢🟢 Successfully added reverse stock split for {symbol}! 🟢🟢")
                    st.cache_data.clear()
                    time.sleep(2)
                    st.rerun()

# --- PAGE: IDX BUYBACK ---
elif page == "IDX Buyback":
    st.header("Upsert: IDX Buyback")
    
    option = st.radio("Action", ["Add New Buyback", "Edit Existing Buyback"])

    if option == "Add New Buyback":
        st.subheader("🆕 Create New Buyback Record")
        
        with st.form("add_new_form",clear_on_submit=True):
            # 1. Basic String/Number Inputs
            symbol = st.text_input("Symbol", placeholder="e.g. BBRI")
            accumulated_shares = st.number_input("Accumulated Shares Purchased", min_value=0, step=1, value=0)
            
            st.divider()

            # 2. Mandate (JSONB)
            st.write("**Mandate Period**")
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date")
            with col2:
                end_date = st.date_input("End Date")
            
            st.divider()

            # 3. Transaction Details (JSONB - Table)
            st.write("**Transaction Details**")
            tx_template = pd.DataFrame(columns=['date', 'share_amount', 'average_price', 'percentage_of_shares'])
            tx_editor = st.data_editor(
                tx_template, 
                num_rows="dynamic", 
                use_container_width=True, 
                key="new_tx_editor"
            )

            st.divider()

            # 5. Company Fund (JSONB - Table)
            st.write("**Company Fund**")
            fund_template = pd.DataFrame([{'allocated_fund': 0.0, 'utilized_fund': 0.0}])
            fund_editor = st.data_editor(
                fund_template, 
                num_rows="dynamic", 
                use_container_width=True, 
                key="new_fund_editor"
            )

            # Form Submission
            submitted = st.form_submit_button("Create New Buyback")

            if submitted:
                if not symbol:
                    st.error("Symbol is required!")
                else:
                    try:
                        # Construct JSONB objects
                        new_data = {
                            "symbol": symbol,
                            "accumulated_shares_purchased": accumulated_shares,
                            "mandate": {
                                "start_date": str(start_date),
                                "end_date": str(end_date)
                            },
                            "transaction_details": tx_editor.to_dict('records'),
                            "company_fund": fund_editor.to_dict('records'),
                            "updated_on": datetime.now().isoformat()
                        }

                        # Clean up empty rows in tables
                        new_data['transaction_details'] = [
                            d for d in new_data['transaction_details'] if any(str(v).strip() for v in d.values() if v is not None)
                        ]
                        new_data['company_fund'] = [
                            d for d in new_data['company_fund'] if any(str(v).strip() for v in d.values() if v is not None)
                        ]

                        # Insert to Supabase
                        supabase.table("idx_buybacks").insert(new_data).execute()
                        
                        st.success(f"🟢🟢 Successfully added buyback for {symbol}! 🟢🟢")
                        st.cache_data.clear()
                        
                        time.sleep(2)
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Error creating record: {e}")

    else:
        # 1. Initialize session state
        if 'is_editing' not in st.session_state:
            st.session_state.is_editing = False
        if 'edit_idx' not in st.session_state:
            st.session_state.edit_idx = None

        @st.cache_data(ttl=60)
        def fetch_buybacks():
            response = supabase.table("idx_buybacks").select("*").execute()
            return response.data

        buybacks_data = fetch_buybacks()

        if buybacks_data:
            df = pd.DataFrame(buybacks_data)
            
            st.subheader("Select a Record to Edit")
            st.dataframe(df, use_container_width=True)
            
            record_index = st.selectbox(
                "Select row index to edit", 
                options=df.index,
                format_func=lambda x: f"Row {x} | Symbol: {df.iloc[x].get('symbol', 'N/A')}"
            )

            # Button to trigger form
            if st.button("Edit Record"):
                st.session_state.is_editing = True
                st.session_state.edit_idx = record_index

            if st.session_state.is_editing:
                st.divider()
                # Retrieve the specific row data
                row_to_edit = df.iloc[st.session_state.edit_idx].to_dict()
                
                with st.form("edit_form"):
                    st.write(f"### 📝 Editing: {row_to_edit.get('symbol', 'N/A')}")
                    
                    updated_values = {}

                    for col in df.columns:
                        # Get value and handle NaN immediately
                        val = row_to_edit.get(col)
                        
                        # Avoid the "ambiguous truth value" error by checking type first
                        if not isinstance(val, (list, dict)) and pd.isna(val):
                            val = None

                        # 1. HIDE BACKEND COLUMNS
                        if col in ['updated_on', 'local_id', 'index']:
                            continue

                        if col in ['symbol']:
                            st.markdown(f"**{col.upper()}:** {val}")
                            updated_values[col] = val
                            continue
                        
                        # 2. READ-ONLY COLUMNS
                        if col in ['id', 'mandate']:
                            st.markdown(f"**{col.upper()}:** {val}")
                            updated_values[col] = val
                            continue
                        
                        # 3. SPECIAL HANDLING: TRANSACTION DETAILS & JSONB (Table Editors)
                        elif col in ['transaction_details', 'company_fund', 'accumulated_shares']:
                            st.write(f"**{col.replace('_', ' ').title()}**")
                            
                            # Normalize val to a list of dicts
                            if isinstance(val, list):
                                val_list = val
                            elif isinstance(val, dict):
                                val_list = [val]
                            else:
                                val_list = []

                            # Build the DataFrame for the editor
                            try:
                                tx_df = pd.DataFrame(val_list)
                                
                                # Ensure column schema if empty
                                if tx_df.empty:
                                    if col == 'transaction_details':
                                        tx_df = pd.DataFrame(columns=['date', 'amount', 'price'])
                                    elif col == 'company_fund':
                                        tx_df = pd.DataFrame(columns=['allocated_fund', 'realized_fund'])
                                    elif col == 'accumulated_shares':
                                        tx_df = pd.DataFrame(columns=['date', 'shares', 'avg_price'])
                            except:
                                tx_df = pd.DataFrame(columns=['data'])
                            
                            # IMPORTANT: key includes edit_idx to force refresh on record change
                            edited_df = st.data_editor(
                                tx_df, 
                                num_rows="dynamic", 
                                use_container_width=True,
                                key=f"editor_{col}_{st.session_state.edit_idx}"
                            )
                            
                            # Store the edited records
                            updated_values[col] = edited_df.to_dict('records')

                        # 4. NUMBERS
                        elif isinstance(val, (int, float)) or col in ['total_buyback', 'budget']:
                            # Ensure it's a number, default to 0
                            try:
                                current_num = int(val) if val is not None else 0
                            except:
                                current_num = 0
                            
                            # Use number_input (float for flexibility)
                            updated_values[col] = st.number_input(f"Edit {col}", value=current_num)
                        
                        # 5. STRINGS
                        else:
                            updated_values[col] = st.text_input(f"Edit {col}", value=str(val) if val is not None else "")

                    # Submit and Cancel buttons
                    col_save, col_cancel = st.columns([1, 5])
                    with col_save:
                        submitted = st.form_submit_button("Save Changes")
                    with col_cancel:
                        cancel = st.form_submit_button("Cancel")

                    if submitted:
                        try:
                            # Manually update the timestamp
                            updated_values['updated_on'] = datetime.now().isoformat()
                            
                            # Clean up empty rows from JSON lists
                            for json_col in ['transaction_details', 'company_fund', 'accumulated_shares']:
                                if json_col in updated_values and updated_values[json_col]:
                                    # A row is valid if at least one value is not None and not an empty string
                                    updated_values[json_col] = [
                                        d for d in updated_values[json_col] 
                                        if any(v is not None and str(v).strip() != "" for v in d.values())
                                    ]
                            
                            # Perform the Upsert
                            supabase.table("idx_buybacks").upsert(updated_values).execute()
                            
                            st.success("🟢🟢 Record updated successfully! 🟢🟢")
                            
                            # Reset state and refresh
                            st.session_state.is_editing = False
                            st.cache_data.clear()
                            time.sleep(2)
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"Error updating record: {e}")

                    if cancel:
                        st.session_state.is_editing = False
                        st.rerun()

        else:
            st.warning("No data found in idx_buybacks.")