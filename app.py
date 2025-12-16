import streamlit as st
import pandas as pd
import us
import re
import requests
from dateutil import parser
from thefuzz import process, fuzz
from typing import List, Any, Optional
from datetime import date, datetime
import json
import io
import zipfile

# --- CONSTANTS ---
API_URL = 'https://api-lrp-dashboard-test-eus.azurewebsites.net/api/v1/taxCredits/calculateTaxCredit'
CURRENT_YEAR = date.today().year

def create_download_zip():
    # 1. THE INSTRUCTIONS FILE
    instructions_text = """
    ANONYMIZATION TOOL INSTRUCTIONS
    ===============================
    1. Install Python if you haven't already.
    2. Place your raw HR CSV file in the same folder as 'anonymizer.py'.
    3. Run the script: python anonymizer.py
    4. Follow the prompts to select your file.
    5. The script will generate a new file called 'clean_data.csv'.
    6. Upload 'clean_data.csv' to the Tax Credit Calculator.

    Usage:
    python anonymize_files.py <file_or_folder_path>

    Arguments:
        file_or_folder_path: Path to a single file or a folder containing files to anonymize

    Features:
        - Processes CSV and Excel (.xls, .xlsx) files
        - Anonymizes SSN, email, names, and dates
        - One-way encrypts identity columns (Employee_ID, empID, ID, Employee ID) for cross-file matching
        - Output files are saved as: input_filename_anonymized.csv (or .xls/.xlsx)
    """

    # 2. THE PYTHON SCRIPT (PASTE YOUR BOSS'S CODE INSIDE THE TRIPLE QUOTES)
    script_content = """


import sys
import os
import pandas as pd
import hashlib
import re
from datetime import datetime
import argparse
from pathlib import Path


def hash_employee_id(employee_id):

    if pd.isna(employee_id):
        return None
    employee_id_str = str(employee_id)
    hash_obj = hashlib.sha256(employee_id_str.encode())
    return hash_obj.hexdigest()


def anonymize_ssn(ssn):
    if pd.isna(ssn):
        return None
    return "XXX-XX-XXXX"


def anonymize_email(email):
    if pd.isna(email):
        return None
    return "anonymous@example.com"


def anonymize_name(name):
    if pd.isna(name):
        return None
    return "ANONYMIZED"


def extract_month_year(date_value):

    if pd.isna(date_value):
        return None
    
    if isinstance(date_value, (pd.Timestamp, datetime)):
        return f"{date_value.year:04d}-{date_value.month:02d}"
    
    if isinstance(date_value, str):
        try:
            dash_match = re.match(r'^(\d{1,2})-(\d{1,2})-(\d{4})$', date_value)
            if dash_match:
                first = int(dash_match.group(1))
                second = int(dash_match.group(2))
                year = int(dash_match.group(3))
                
                if first > 12:
                    return f"{year:04d}-{second:02d}"
                elif second > 12:
                    return f"{year:04d}-{first:02d}"
                else:
                    # Try mm-dd-yyyy first
                    try:
                        dt = datetime.strptime(date_value, '%m-%d-%Y')
                        return f"{dt.year:04d}-{dt.month:02d}"
                    except ValueError:
                        # Fallback to dd-mm-yyyy
                        try:
                            dt = datetime.strptime(date_value, '%d-%m-%Y')
                            return f"{dt.year:04d}-{dt.month:02d}"
                        except ValueError:
                            pass
            
            # Try common date formats
            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']:
                try:
                    dt = datetime.strptime(date_value, fmt)
                    return f"{dt.year:04d}-{dt.month:02d}"
                except ValueError:
                    continue
            # If no format matches, try to extract year and month from string
            # Look for YYYY-MM pattern first
            year_month_match = re.search(r'(\d{4})[-/](\d{1,2})', date_value)
            if year_month_match:
                year = int(year_month_match.group(1))
                month = int(year_month_match.group(2))
                return f"{year:04d}-{month:02d}"
            # Fallback: try to extract just year (assume month 01)
            year_match = re.search(r'\d{4}', date_value)
            if year_match:
                year = int(year_match.group())
                return f"{year:04d}-01"
        except:
            pass
    
    return None


def find_identity_columns(df):

    identity_columns = []

    for col in df.columns:
        col_lower = col.lower().strip()
        # Normalize separators to spaces for simpler word-boundary checks
        normalized = re.sub(r'[^a-z0-9]+', ' ', col_lower).strip()
        tokens = normalized.split()

        # Explicit identity patterns
        explicit_matches = {
            'id',
            'empid',
            'employee id',
            'employeeid',
            'employee number',
            'emp number',
            'emp no',
            'employee no',
        }

        # Word-boundary checks to avoid partial matches (e.g., "middle" contains "id")
        is_identity = (
            normalized in explicit_matches
            or ('employee' in tokens and 'id' in tokens)
            or ('employee' in tokens and 'number' in tokens)
            or ('employee' in tokens and 'no' in tokens)
        )

        if is_identity:
            identity_columns.append(col)

    return identity_columns


def anonymize_dataframe(df):

    df_anon = df.copy()
    
    # Find and hash identity columns (one-way encryption for cross-file matching)
    identity_columns = find_identity_columns(df_anon)
    for col in identity_columns:
        df_anon[col] = df_anon[col].apply(hash_employee_id)
        print(f"  One-way encrypted identity column: {col}")
    
    # Anonymize SSN columns (SSN, Dependent SSN, etc.)
    ssn_columns = [col for col in df_anon.columns if 'ssn' in col.lower()]
    for col in ssn_columns:
        df_anon[col] = df_anon[col].apply(anonymize_ssn)
    
    # Anonymize email columns
    email_columns = [col for col in df_anon.columns if 'email' in col.lower()]
    for col in email_columns:
        df_anon[col] = df_anon[col].apply(anonymize_email)
    
    # Anonymize name columns (Employee Name, Employee First Name, Employee Middle Name, Employee Last Name,
    # Dependent Name, Dependent First Name, Dependent Middle Name, Dependent Last Name)
    # Exclude Company Name and Plan Name from anonymization
    excluded_name_columns = ['company name', 'plan name']
    name_columns = [col for col in df_anon.columns if any(
        keyword in col.lower() for keyword in ['name', 'first name', 'middle name', 'last name', 'full name']
    ) and col.lower() not in excluded_name_columns]
    for col in name_columns:
        df_anon[col] = df_anon[col].apply(anonymize_name)
    
    # Extract month and year from date columns (all dates, DOB, birth dates including Dependent DOB)
    date_columns = [col for col in df_anon.columns if any(
        keyword in col.lower() for keyword in ['date', 'dob', 'birth']
    )]
    for col in date_columns:
        df_anon[col] = df_anon[col].apply(extract_month_year)
    
    return df_anon


def read_data_file(file_path):

    file_ext = Path(file_path).suffix.lower()
    
    if file_ext == '.csv':
        return pd.read_csv(file_path)
    elif file_ext in ['.xls', '.xlsx']:
        return pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_ext}. Supported formats: .csv, .xls, .xlsx")


def save_data_file(df, output_path, original_ext):

    if original_ext == '.csv':
        df.to_csv(output_path, index=False)
    elif original_ext in ['.xls', '.xlsx']:
        df.to_excel(output_path, index=False)
    else:
        raise ValueError(f"Unsupported file format: {original_ext}")


def process_file(file_path):

    try:
        print(f"\nProcessing: {file_path}")
        
        # Read the file
        df = read_data_file(file_path)
        print(f"  Read {len(df)} rows, {len(df.columns)} columns")
        
        # Anonymize the dataframe
        anonymized_df = anonymize_dataframe(df)
        
        # Generate output filename
        file_path_obj = Path(file_path)
        original_ext = file_path_obj.suffix
        output_filename = f"{file_path_obj.stem}_anonymized{original_ext}"
        output_path = file_path_obj.parent / output_filename
        
        # Save the anonymized file
        save_data_file(anonymized_df, output_path, original_ext)
        print(f"  Saved anonymized data to: {output_path}")
        
        return True
    except Exception as e:
        print(f"  Error processing {file_path}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return False


def get_data_files(path):

    path_obj = Path(path)
    
    if path_obj.is_file():
        # Single file
        return [path_obj]
    elif path_obj.is_dir():
        # Folder - find all CSV and Excel files
        data_files = []
        for ext in ['*.csv', '*.xls', '*.xlsx']:
            data_files.extend(path_obj.glob(ext))
        return sorted(data_files)
    else:
        raise FileNotFoundError(f"Path not found: {path}")


def main():
    parser = argparse.ArgumentParser(
        description='Anonymize PII data in CSV or Excel files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('path', help='Path to a single file or a folder containing files to anonymize')
    
    args = parser.parse_args()
    
    try:
        # Get list of files to process
        data_files = get_data_files(args.path)
        
        if not data_files:
            print(f"No data files found in: {args.path}")
            print("Supported formats: .csv, .xls, .xlsx")
            sys.exit(1)
        
        print(f"Found {len(data_files)} file(s) to process")
        
        # Process each file
        success_count = 0
        for file_path in data_files:
            if process_file(file_path):
                success_count += 1
        
        print(f"\n{'='*60}")
        print(f"Processing complete: {success_count}/{len(data_files)} file(s) processed successfully")
        print(f"{'='*60}")
        
        if success_count < len(data_files):
            sys.exit(1)
            
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()


    """

    # Create ZIP in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        zf.writestr("instructions.txt", instructions_text)
        zf.writestr("anonymizer.py", script_content)
    
    return zip_buffer.getvalue()


# ==========================================
# HELPER FUNCTIONS
# ==========================================

def load_data(uploaded_file):
    try:
        if uploaded_file.name.endswith('.csv'):
            return pd.read_csv(uploaded_file)
        else:
            return pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"Error loading file: {e}")
        return None

def extract_state_code(text):
    if not isinstance(text, (str, int, float)): return "N/A"
    text = str(text).strip().upper()
    if len(text) == 2 and us.states.lookup(text):
        return us.states.lookup(text).abbr
    state_obj = us.states.lookup(text)
    if state_obj: return state_obj.abbr
    match = re.search(r'\b([A-Z]{2})\b\s+\d{5}', text)
    if match:
        potential_state = match.group(1)
        if us.states.lookup(potential_state): return potential_state
    return "N/A"

def calculate_wage(value):
    """Cleans string to float."""
    if pd.isna(value): return None
    if isinstance(value, (int, float)): return float(value)
    val_str = str(value).strip().lower()
    try: 
        val_str = re.sub(r'[$,]', '', val_str)
        if not val_str: return None
        return float(val_str)
    except ValueError: 
        return None

def suggest_column(options: List[str], keywords: List[str]) -> int:
    available_cols = options[1:]
    best_match_col = None
    highest_score = 0
    
    for col in available_cols:
        col_lower = str(col).lower()
        for kw in keywords:
            if len(kw) < 2: continue
            score = fuzz.partial_ratio(kw, col_lower)
            if kw in col_lower: score += 10
            if kw == col_lower: return options.index(col)
            if score > highest_score:
                highest_score = score
                best_match_col = col
    
    # if best_match_col:
    #     st.sidebar.write(f"🔍 Search: **{keywords[0]}** | Found: `{best_match_col}` | Score: `{highest_score}`")
    
    if highest_score < 80 or best_match_col is None: return 0
    try:
        return options.index(best_match_col)
    except ValueError:
        return 0

def parse_full_address(address_string: str) -> dict:
    address_string = str(address_string).strip()
    state = extract_state_code(address_string)
    zip_match = re.search(r'(\d{5})(-\d{4})?$', address_string)
    zip_code = zip_match.group(1) if zip_match else ""
    parts = [p.strip() for p in address_string.split(',')]
    city = ""
    street = address_string
    if len(parts) >= 2:
        city = parts[-2]
        street = ", ".join(parts[:-2])
        if not street: street = parts[0]
        city = re.sub(r'[A-Z]{2}\s*\d{5}.*$', '', city).strip()

    return {
        "street": street or "", "city": city or "", "zip": zip_code or "",
        "state": state or "", "country": "USA"
    }

# ==========================================
# MAIN APP
# ==========================================

st.set_page_config(page_title="Tax Credit Saver", layout="wide")

# --- HEADER & DOWNLOAD SECTION ---
col_head1, col_head2 = st.columns([3, 1])
with col_head1:
    st.title("💰 Tax Credit Calculator")
with col_head2:
    st.write("") # Spacer
    st.write("") # Spacer
    zip_bytes = create_download_zip()
    st.download_button(
        label="⬇️ Download Anonymizer Tool",
        data=zip_bytes,
        file_name="anonymizer_tools.zip",
        mime="application/zip",
        help="Download the Python script and instructions to clean your data before uploading."
    )

uploaded_file = st.file_uploader("Upload Anonymized Data (CSV/Excel)", type=['csv', 'xlsx', 'xls'])

if uploaded_file:
    df = load_data(uploaded_file)
    
    if df is not None:
        st.subheader("1. Data Preview")
        st.dataframe(df)
        st.write("---")
        # --- CONFIGURATION ---
        st.subheader("2. Mapping")
        
        c_conf1, c_conf2 = st.columns(2)
        with c_conf1:
            wage_freq = st.radio(
                "Select Wage Frequency in File:", 
                ("Annually", "Monthly", "Bi-Weekly", "Weekly", "Daily"),
                index=0,
                horizontal=True
            )
        with c_conf2:
            addr_mode = st.radio("Address Format:", ("Consolidated Address Column", "Separate Columns"), index=1)

        
        COL_OPTIONS = ["NA"] + list(df.columns)
        
        kw_st = [
            'street', 'address line 1', 'residence street',
            'street address', 'street name', 'addr1'
        ]
        kw_city = [
            'city','town', 'residence city',
            'municipality', 'locality'
        ]
        kw_state = [
            'state', 'st', 'province', 'region', 'territory', 'residence state',
            'state code', 'state name'
        ]
        kw_zip = [
            'zip', 'postal code', 'zip code',
            'postcode', 'pin code'
        ]
        

        kw_id = [
            'id', 'employee id', 'emp id', 'staff id', 'number', 'worker id',
            'employee number', 'emp number', 'personnel id', 'badge id'
        ]

        kw_full_addr = [
            'address', 'employee address', 'home address', 'full address',
            'residential address', 'mailing address', 'complete address'
        ]

        kw_age = [
            'age', 'dob', 'date of birth',
            'birth date', 'birth year'
        ]

        kw_hire = [
            'hire date', 'start date', 'date hired', 'joining date',
            'date of joining', 'employment start'
        ]

        kw_term = [
            'termination date', 'exit date', 'separation date', 'end date',
            'relieving date', 'last working day', 'work ending date'
        ]

        kw_wage = [
            'wage', 'salary', 'pay rate', 'gross pay', 'compensation',
            'base salary', 'monthly salary', 'annual salary', 'rate of pay'
        ]

        kw_co_addr = [
            'company address', 'office address', 'corporate address', 'business address',
            'registered office', 'head office address'
        ]

        def get_col(lbl, kws, k, req=False):
            return st.selectbox(f"{lbl} {'*' if req else ''}", COL_OPTIONS, index=suggest_column(COL_OPTIONS, kws), key=k)

        # MAPPING
        c1, c2, c3, c4 = st.columns(4)
        with c1: col_id = get_col("Employee ID", kw_id, 'cid', True)
        with c2: col_hire = get_col("Hire Date", kw_hire, 'chire', True)
        with c3: col_wage = get_col("Wage", kw_wage, 'cwage', True)
        with c4: col_term = get_col("Termination Date", kw_term, 'cterm')

        
        col_st, col_city, col_state, col_zip, col_full = None, None, None, None, None
        if addr_mode == "Consolidated Address Column":
            c_addr1, c_age = st.columns([2, 1])
            with c_addr1: col_full = get_col("Full Address", kw_full_addr, 'cfull', True)
            with c_age: col_age = get_col("Age", kw_age, 'cage')
        else:
            c_st, c_cit, c_sta, c_zip, c_age = st.columns(5)
            with c_st: col_st = get_col("Street", kw_st, 'cst', True)
            with c_cit: col_city = get_col("City", kw_city, 'ccity', True)
            with c_sta: col_state = get_col("State", kw_state, 'cstate', True)
            with c_zip: col_zip = get_col("ZIP", kw_zip, 'czip', True)
            with c_age: col_age = get_col("Age", kw_age, 'cage')
        
        st.write("---")
        st.markdown("### Company Data")
        col_co_addr = get_col("Company Address Column", kw_co_addr, 'ccomp')
        
        man_st, man_city, man_zip, man_state = None, None, None, None
        if col_co_addr == "NA":
            st.info("Enter Company Address Manually:")
            mc1, mc2, mc3, mc4 = st.columns(4)
            with mc1: man_st = st.text_input("Street", key='mst')
            with mc2: man_city = st.text_input("City", key='mct')
            with mc3: man_state = st.selectbox("State", ["NY", "CA"] + [s.abbr for s in us.states.STATES], key='mstt')
            with mc4: man_zip = st.text_input("ZIP", key='mzp')

        st.divider()

        if st.button("Calculate Tax Credits", type="primary"):
            # Validation
            reqs = [col_id, col_hire, col_wage]
            if addr_mode == "Consolidated Address Column": reqs.append(col_full)
            else: reqs.extend([col_st, col_city, col_state, col_zip])
            
            if "NA" in reqs or (col_co_addr == "NA" and not man_st):
                st.error("Missing required mappings.")
            else:
                process_df = df[df[col_id].notna()].copy()
                total_employees = len(process_df)
                
                st.subheader("Savings Dashboard")
                m1, m2, m3, m4, m5 = st.columns(5)
                
                with m1: wotc_ph = st.empty()
                with m2: fez_ph = st.empty()
                with m3: past_ph = st.empty()
                with m4: future_ph = st.empty()
                with m5: grand_total_ph = st.empty()
                
                # Initial State
                wotc_ph.metric("WOTC Savings", "$0.00")
                fez_ph.metric("FEZ Savings", "$0.00")
                past_ph.metric("Past Credits", "$0.00")
                future_ph.metric("Future Credits (per year)", "$0.00")
                grand_total_ph.metric("GRAND TOTAL", "$0.00")
                
                prog_bar = st.progress(0)
                status_text = st.empty()
                table_placeholder = st.empty()

                total_wotc = 0.0
                total_fez = 0.0
                total_past = 0.0
                total_future = 0.0
                results_data = []

                for index, row in process_df.iterrows():
                    
                    raw_hire = row.get(col_hire)
                    raw_term = row.get(col_term) if col_term != "NA" else None

                    if pd.isna(raw_hire): continue
                    
                    try:
                        dt_hire_original = pd.to_datetime(raw_hire)
                        hire_year = dt_hire_original.year
                    except: continue 

                    dt_term_original = None
                    term_year = 9999 
                    if raw_term and pd.notna(raw_term):
                        try:
                            dt_term_original = pd.to_datetime(raw_term)
                            term_year = dt_term_original.year
                        except: pass

                    end_loop_year = min(CURRENT_YEAR, term_year)
                    if hire_year > CURRENT_YEAR: continue

                    raw_wage_val = calculate_wage(row.get(col_wage))
                    annualized_wage = 0
                    
                    if raw_wage_val is not None:
                        if wage_freq == "Annually":
                            annualized_wage = raw_wage_val
                        elif wage_freq == "Monthly":
                            annualized_wage = raw_wage_val * 12
                        elif wage_freq == "Bi-Weekly":
                            annualized_wage = raw_wage_val * 26
                        elif wage_freq == "Weekly":
                            annualized_wage = raw_wage_val * 52
                        elif wage_freq == "Daily":
                            annualized_wage = raw_wage_val * 260 

                    if addr_mode == "Consolidated Address Column":
                        emp_addr = parse_full_address(row.get(col_full, ""))
                    else:
                        emp_addr = {
                            "street": str(row.get(col_st, "")),
                            "city": str(row.get(col_city, "")),
                            "zip": str(row.get(col_zip, "")),
                            "state": extract_state_code(row.get(col_state, "")),
                            "country": "USA"
                        }
                    
                    if col_co_addr != "NA": comp_addr = parse_full_address(row.get(col_co_addr, ""))
                    else: comp_addr = {"street": man_st, "city": man_city, "zip": man_zip, "state": man_state, "country": "USA"}

                    val_age = 0
                    if col_age != "NA": 
                        try: val_age = int(float(row.get(col_age)))
                        except: pass

                    # --- SPLIT LOOP: YEAR BY YEAR ---
                    for year in range(hire_year, end_loop_year + 1):
                        
                        is_first_year = (year == hire_year)
                        
                        # Set API Dates
                        if is_first_year: api_hire_date_str = dt_hire_original.strftime("%Y-%m-%d")
                        else: api_hire_date_str = f"{year}-01-01"

                        api_term_date_str = None
                        if year < end_loop_year:
                            api_term_date_str = f"{year}-12-31"
                        elif year == end_loop_year:
                            if dt_term_original and year == term_year:
                                api_term_date_str = dt_term_original.strftime("%Y-%m-%d")
                            else:
                                api_term_date_str = None

                        payload = {
                            "employeeStreet": emp_addr.get("street", ""),
                            "employeeCity": emp_addr.get("city", ""),
                            "employeeZip": emp_addr.get("zip", ""),
                            "employeeState": emp_addr.get("state", ""),
                            "employeeCountry": "USA",
                            "companyStreet": comp_addr.get("street", ""),
                            "companyCity": comp_addr.get("city", ""),
                            "companyZip": comp_addr.get("zip", ""),
                            "companyState": comp_addr.get("state", ""),
                            "companyCountry": "USA",
                            "employeeAge": val_age,
                            "hireDate": api_hire_date_str,
                            "terminationDate": api_term_date_str,
                            "wage": annualized_wage
                        }

                        status_text.text(f"Processing {row.get(col_id)} for Year {year}...")
                        
                        raw_wotc, raw_fez, raw_past, raw_future = 0, 0, 0, 0
                        error_msg = None

                        try:
                            headers = {'Content-Type': 'application/json'}
                            res = requests.post(API_URL, headers=headers, json=payload, timeout=8)
                            if res.status_code == 200:
                                d = res.json()
                                raw_wotc = d.get('wotcCredit', 0) or 0
                                raw_fez = d.get('fezCredit', 0) or 0
                                raw_past = d.get('pastCredit', 0) or 0
                                raw_future = d.get('futureCredit', 0) or 0
                            else: error_msg = f"HTTP {res.status_code}"
                        except Exception as e: error_msg = str(e)
                        final_wotc = float(raw_wotc)
                        final_fez = float(raw_fez)

                        # Rule: Year 1 shows all credits.
                        # Rule: Year 2+ Hides WOTC, and adds 1200 to FEZ if both were applicable
                        if not is_first_year:
                            final_wotc = 0.0
                            if float(raw_wotc) > 0 and float(raw_fez) > 0:
                                final_fez += 1200.0

                        # --- AGGREGATE ---
                        total_wotc += final_wotc
                        total_fez += final_fez
                        total_past += float(raw_past)
                        total_future += float(raw_future)
                        grand_total = total_wotc + total_fez + total_past + total_future
                        # grand_total = total_wotc + total_fez  + total_future

                        wotc_ph.metric("WOTC Savings", f"${total_wotc:,.2f}")
                        fez_ph.metric("FEZ Savings", f"${total_fez:,.2f}")
                        past_ph.metric("Past Credits", f"${total_past:,.2f}")
                        future_ph.metric("Future Credits (per year)", f"${total_future:,.2f}")
                        grand_total_ph.metric("GRAND TOTAL", f"${grand_total:,.2f}")

                        row_result = {
                            "Employee_ID": row.get(col_id),
                            "Tax_Year": year,
                            "API_Hire_Date": api_hire_date_str,
                            "API_Term_Date": api_term_date_str,
                            "WOTC": final_wotc, "FEZ": final_fez, 
                            "Past": raw_past, "Future": raw_future,
                            "Total": final_wotc + final_fez + raw_past + raw_future,
                            "Wage_Used": annualized_wage,
                            "Status": "Error" if error_msg else "Success"
                        }
                        if error_msg: row_result["Error"] = error_msg
                        results_data.append(row_result)
                        
                        
                        table_placeholder.dataframe(pd.DataFrame(results_data), width='stretch')
                    
                    prog_bar.progress((index + 1) / total_employees)

                status_text.success("Complete!")
                final_csv = pd.DataFrame(results_data).to_csv(index=False).encode('utf-8')
                st.download_button("Download Detailed CSV", final_csv, "results_split_years.csv", "text/csv")
