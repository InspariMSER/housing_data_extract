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
st.title("🏠 Boligoversigt")

# Display scoring information
with st.expander("ℹ️ Information om scoring systemet"):
    st.write("**Scoring Algoritme - Samlet score (Max: 80 point)**")
    st.write("Hvert hus får en score baseret på 8 forskellige faktorer. Hver faktor gives en score fra 0-10 point, som tilsammen giver en maksimal score på 80 point.")
    st.write("• **Energiklasse**: Bedre energiklasse = højere score (A=10, B=8, C=6, osv.)")
    st.write("• **Togstation afstand**: Jo tættere på togstation, jo højere score")
    st.write("• **Grundstørrelse**: Større grund = højere score sammenlignet med andre i samme område")
    st.write("• **Husstørrelse**: Større hus = højere score sammenlignet med andre i samme område")
    st.write("• **Pris effektivitet**: Lavere m²-pris = højere score sammenlignet med andre i samme område")
    st.write("• **Byggeår**: Nyere hus = højere score sammenlignet med andre i samme område")
    st.write("• **Kælderstørrelse**: Større kælder = højere score sammenlignet med andre i samme område")
    st.write("• **Dage på markedet**: Færre dage til salg = højere score sammenlignet med andre i samme område")

# Create filters on the right side of the screen
zip_codes = sorted(listings_scored['zip_code'].unique())
show_city_in_zip = st.sidebar.checkbox("Vis kun boliger med hvor bynavn er det samme som postnummeret", value=True)
show_already_seen_houses = st.sidebar.checkbox("Vis huse der allerede er markeret som set", value=False)
selected_zip_codes = st.sidebar.multiselect("Vælg postnumre", zip_codes, default=['8370','8382'])

# Enhanced filters
st.sidebar.subheader("📊 Basis Filtre")
max_budget = st.sidebar.number_input("Maksimalt budget", min_value=0, max_value=15000000, value=4000000, step=100000)
min_rooms = st.sidebar.number_input("Minimum antal værelser", min_value=0, max_value=15, value=5)
min_m2 = st.sidebar.number_input("Minimum m²", min_value=0, max_value=1000, value=150)
min_score = st.sidebar.number_input("Minimum samlet score", min_value=0, max_value=80, value=0, step=1)

# New enhanced filters
st.sidebar.subheader("🏗️ Boligdetaljer")
min_build_year = st.sidebar.number_input("Minimum byggeår", min_value=1900, max_value=2025, value=1960)
energy_classes = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'UNKNOWN']
selected_energy_classes = st.sidebar.multiselect("Energiklasser", energy_classes, default=['A', 'B', 'C', 'D'])

min_lot_size = st.sidebar.number_input("Minimum grundstørrelse (m²)", min_value=0, max_value=5000, value=0)
min_basement_size = st.sidebar.number_input("Minimum kælderstørrelse (m²)", min_value=0, max_value=500, value=0)

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
if 'UNKNOWN' not in selected_energy_classes:
    energy_filter = (filtered_listings['energy_class'].isin(selected_energy_classes)) | \
                   (filtered_listings['energy_class'].isna() & ('UNKNOWN' in selected_energy_classes))
    filtered_listings = filtered_listings[energy_filter]

filtered_listings = filtered_listings[filtered_listings['lot_size'].fillna(0) >= min_lot_size]
filtered_listings = filtered_listings[filtered_listings['basement_size'].fillna(0) >= min_basement_size]

if show_city_in_zip:
    filtered_listings = filtered_listings[filtered_listings['is_in_zip_code_city'] == show_city_in_zip]

# Sort by total score (highest first)
filtered_listings = filtered_listings.sort_values(by='total_score', ascending=False)

# Display results
if selected_zip_codes:  
    st.write(f"**Fandt {len(filtered_listings)} boliger der matcher dine kriterier**")
    
    # Create tabs for different views
    data_tab, scores_tab, seen_tab = st.tabs(["🏠 Boliger", "📊 Pointdetaljer", "🔍 Alllerede sete huse"])
    
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
                "total_score": st.column_config.NumberColumn("Samlet score", format="%.1f"),
                "price": st.column_config.NumberColumn("Pris", format="%d"),
                "m2": st.column_config.NumberColumn("m²", format="%d"),
                "rooms": st.column_config.NumberColumn("Værelser", format="%d"),
                "built": st.column_config.TextColumn("Byggeår"),
                "energy_class": st.column_config.TextColumn("Energiklasse"),
                "lot_size": st.column_config.NumberColumn("Grundstørrelse", format="%d"),
                "basement_size": st.column_config.NumberColumn("Kælder", format="%d"),
                "days_on_market": st.column_config.NumberColumn("Dage på marked", format="%d"),
                "full_address": st.column_config.TextColumn("Adresse", width="large")
            }
        )

        ouid_input = st.text_input("Indtast ouID'er adskilt af kommaer")
        if st.button("Tilføj til sete huse"):
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
                st.success("ouID'er tilføjet til sete huse")
            else:
                st.error("Indtast venligst gyldige ouID'er")
    
    with scores_tab:
        # Score breakdown details
        score_columns = ['full_address', 'score_price_efficiency', 'score_house_size', 
                       'score_build_year', 'score_energy', 'score_lot_size', 
                       'score_basement', 'score_days_market', 'total_score', 'score_train_distance']
        
        st.dataframe(
            data=filtered_listings[score_columns], 
            height=600, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "score_price_efficiency": st.column_config.NumberColumn("Pris", format="%.1f"),
                "score_house_size": st.column_config.NumberColumn("Størrelse", format="%.1f"),
                "score_build_year": st.column_config.NumberColumn("År", format="%.1f"),
                "score_energy": st.column_config.NumberColumn("Energi", format="%.1f"),
                "score_lot_size": st.column_config.NumberColumn("Grund", format="%.1f"),
                "score_basement": st.column_config.NumberColumn("Kælder", format="%.1f"),
                "score_days_market": st.column_config.NumberColumn("Marked", format="%.1f"),
                "total_score": st.column_config.NumberColumn("Samlet", format="%.1f"),
                "score_train_distance": st.column_config.NumberColumn("Tog", format="%.1f"),
                "full_address": st.column_config.TextColumn("Adresse", width="medium")
            }
        )
    
    with seen_tab:
        # Seen Houses section
        st.subheader("🔍 Sete Huse")
        
        sqlquery = """select b.full_address from mser_catalog.housing.seen_houses as a INNER JOIN mser_catalog.housing.listings_scored as b on a.ouID=b.ouID"""
        houses_joined = sqlQuery(sqlquery)
        if st.button("Opdater data for sete huse"):
            houses_joined = sqlQuery(sqlquery)
        st.dataframe(data=houses_joined, height=400, use_container_width=True, hide_index=True)
else:
    st.write("Vælg venligst mindst ét postnummer.")