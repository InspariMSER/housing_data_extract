import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

def sqlQuery(query: str) -> pd.DataFrame:
    cfg = Config()  # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

st.set_page_config(layout="wide")

@st.cache_data(ttl=30)  # only re-query if it's been 30 seconds
def get_listings_Data():
    # Query data from the specified tables with enhanced scoring details
    listings_scored = sqlQuery("""
        select full_address, price, m2, m2_price, rooms, cast(built as string) as built, 
               zip_code, days_on_market, is_in_zip_code_city, total_score, ouID,
               energy_class, lot_size, basement_size, 
               score_energy, score_train_distance, score_lot_size, 
               score_house_size, score_price_efficiency, score_build_year,
               score_basement, score_days_market
        from mser_catalog.housing.listings_scored
    """)
    return listings_scored

def get_seen_houses():
    seen_houses = sqlQuery("select ouID from mser_catalog.housing.seen_houses")
    return seen_houses

listings_scored = get_listings_Data()
seen_houses = get_seen_houses()

# Main app content
st.title("🏠 House Listings - Enhanced Scoring")

# Display scoring information
with st.expander("ℹ️ Scoring System Information"):
    st.write("**Enhanced Scoring Algorithm (Max: 80 points)**")
    col1, col2 = st.columns(2)
    with col1:
        st.write("• Energy Class: 10 pts max (GLOBAL)")
        st.write("• Train Distance: 10 pts max (GLOBAL)")
        st.write("• Lot Size: 10 pts max (RELATIVE)")
        st.write("• House Size: 10 pts max (RELATIVE)")
    with col2:
        st.write("• Price Efficiency: 10 pts max (RELATIVE)")
        st.write("• Build Year: 10 pts max (RELATIVE)")
        st.write("• Basement Size: 10 pts max (RELATIVE)")
        st.write("• Days on Market: 10 pts max (RELATIVE)")

# Create filters on the right side of the screen
    zip_codes = sorted(listings_scored['zip_code'].unique())
    show_city_in_zip = st.sidebar.checkbox("Show only listings with city in Zip Code", value=True)
    show_already_seen_houses = st.sidebar.checkbox("Show houses already marked as seen", value=False)
    selected_zip_codes = st.sidebar.multiselect("Select Zip Codes", zip_codes, default=zip_codes)
    
    # Enhanced filters
    st.sidebar.subheader("📊 Basic Filters")
    max_budget = st.sidebar.number_input("Maximum Budget", min_value=0, max_value=15000000, value=4000000, step=100000)
    min_rooms = st.sidebar.number_input("Minimum Rooms", min_value=0, max_value=15, value=5)
    min_m2 = st.sidebar.number_input("Minimum M²", min_value=0, max_value=1000, value=150)
    min_score = st.sidebar.number_input("Minimum Total Score", min_value=0.0, max_value=80.0, value=0.0, step=0.5)
    
    # New enhanced filters
    st.sidebar.subheader("🏗️ Property Details")
    min_build_year = st.sidebar.number_input("Minimum Build Year", min_value=1900, max_value=2025, value=1960)
    energy_classes = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'Unknown']
    selected_energy_classes = st.sidebar.multiselect("Energy Classes", energy_classes, default=energy_classes)
    
    min_lot_size = st.sidebar.number_input("Minimum Lot Size (m²)", min_value=0, max_value=5000, value=0)
    min_basement_size = st.sidebar.number_input("Minimum Basement Size (m²)", min_value=0, max_value=500, value=0)
    
    # Score breakdown filters
    st.sidebar.subheader("🎯 Score Filters")
    min_energy_score = st.sidebar.slider("Min Energy Score", 0.0, 10.0, 0.0, 0.5)
    min_train_score = st.sidebar.slider("Min Train Distance Score", 0.0, 10.0, 0.0, 0.5)

    # Join the seen_houses onto the listings_scored dataframe
    listings_scored = listings_scored.merge(seen_houses.assign(is_seen=True), on='ouID', how='left').fillna({'is_seen': False})

    # Filter dataframe based on seen_houses
    if not show_already_seen_houses:
        filtered_listings = listings_scored[~listings_scored['is_seen']]
    else:
        filtered_listings = listings_scored      

    # Apply all filters
    filtered_listings = filtered_listings[filtered_listings['zip_code'].isin(selected_zip_codes)]
    filtered_listings = filtered_listings[filtered_listings['price'] <= max_budget]
    filtered_listings = filtered_listings[filtered_listings['rooms'] >= min_rooms]
    filtered_listings = filtered_listings[filtered_listings['m2'] >= min_m2]
    filtered_listings = filtered_listings[filtered_listings['total_score'] >= min_score]
    
    # Enhanced filters
    filtered_listings = filtered_listings[filtered_listings['built'].astype(float) >= min_build_year]
    
    # Energy class filter (handle missing values)
    if 'Unknown' not in selected_energy_classes:
        energy_filter = (filtered_listings['energy_class'].isin(selected_energy_classes)) | \
                       (filtered_listings['energy_class'].isna() & ('Unknown' in selected_energy_classes))
        filtered_listings = filtered_listings[energy_filter]
    
    filtered_listings = filtered_listings[filtered_listings['lot_size'].fillna(0) >= min_lot_size]
    filtered_listings = filtered_listings[filtered_listings['basement_size'].fillna(0) >= min_basement_size]
    
    # Score filters
    filtered_listings = filtered_listings[filtered_listings['score_energy'].fillna(0) >= min_energy_score]
    filtered_listings = filtered_listings[filtered_listings['score_train_distance'].fillna(0) >= min_train_score]

    if show_city_in_zip:
        filtered_listings = filtered_listings[filtered_listings['is_in_zip_code_city'] == show_city_in_zip]
    
    # Sort by total score (highest first)
    filtered_listings = filtered_listings.sort_values(by='total_score', ascending=False)

    # Display results
    if selected_zip_codes:  
        st.write(f"**Found {len(filtered_listings)} listings matching your criteria**")
        
        # Create tabs for different views
        data_tab, scores_tab = st.tabs(["🏠 Property Data", "📊 Score Breakdown"])
        
        with data_tab:
            # Main property information
            property_columns = ['full_address', 'price', 'm2', 'rooms', 'built', 
                              'energy_class', 'lot_size', 'basement_size', 'days_on_market', 'total_score']
            
            st.dataframe(
                data=filtered_listings[property_columns], 
                height=600, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "total_score": st.column_config.NumberColumn("Total Score", format="%.1f"),
                    "price": st.column_config.NumberColumn("Price", format="%d"),
                    "lot_size": st.column_config.NumberColumn("Lot Size", format="%d"),
                    "basement_size": st.column_config.NumberColumn("Basement", format="%d"),
                    "full_address": st.column_config.TextColumn("Address", width="large")
                }
            )
        
        with scores_tab:
            # Score breakdown details
            score_columns = ['full_address', 'total_score', 'score_energy', 'score_train_distance', 
                           'score_build_year', 'score_house_size', 'score_price_efficiency', 
                           'score_lot_size', 'score_basement', 'score_days_market']
            
            st.dataframe(
                data=filtered_listings[score_columns], 
                height=600, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "total_score": st.column_config.NumberColumn("Total", format="%.1f"),
                    "score_energy": st.column_config.NumberColumn("Energy", format="%.1f"),
                    "score_train_distance": st.column_config.NumberColumn("Train", format="%.1f"),
                    "score_build_year": st.column_config.NumberColumn("Year", format="%.1f"),
                    "score_house_size": st.column_config.NumberColumn("Size", format="%.1f"),
                    "score_price_efficiency": st.column_config.NumberColumn("Price", format="%.1f"),
                    "score_lot_size": st.column_config.NumberColumn("Lot", format="%.1f"),
                    "score_basement": st.column_config.NumberColumn("Basement", format="%.1f"),
                    "score_days_market": st.column_config.NumberColumn("Market", format="%.1f"),
                    "full_address": st.column_config.TextColumn("Address", width="medium")
                }
            )
    else:
        st.write("Please select at least one zip code.")

    ouid_input = st.text_input("Enter ouIDs separated by commas")
    if st.button("Add to Seen Houses"):
        ouids = [ouid.strip() for ouid in ouid_input.split(",") if ouid.strip()]
        if ouids:
            for ouid in ouids:
                query = f"""
                    MERGE INTO mser_catalog.housing.seen_houses AS target
                    USING (SELECT '{ouid}' AS ouID) AS source
                    ON target.ouID = source.ouID
                    WHEN NOT MATCHED THEN
                    INSERT (ouID) VALUES (source.ouID)
                """
                sqlQuery(query)
            st.success("ouIDs added to Seen Houses")
        else:
            st.error("Please enter valid ouIDs")

# Seen Houses section at the bottom
st.subheader("🔍 Seen Houses")
sqlquery = """select b.full_address from mser_catalog.housing.seen_houses as a INNER JOIN mser_catalog.housing.listings_scored as b on a.ouID=b.ouID"""
houses_joined = sqlQuery(sqlquery)
if st.button("Refresh Seen Houses Data"):
    houses_joined = sqlQuery(sqlquery)
st.dataframe(data=houses_joined, height=400, use_container_width=True, hide_index=True)