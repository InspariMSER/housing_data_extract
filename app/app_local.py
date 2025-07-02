"""
Local Housing Data Streamlit App
Migrated from Databricks to work with local DuckDB
"""

import sys
from pathlib import Path
import streamlit as st

# Add src directory to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from database_local import HousingDataDB

st.set_page_config(layout="wide", page_title="🏠 Boligoversigt - Lokal Version")

@st.cache_data(ttl=30)  # Cache for 30 seconds
def get_listings_data():
    """Get listings data from local database."""
    db = HousingDataDB()
    try:
        listings_scored = db.get_listings_for_streamlit()
        return listings_scored
    finally:
        db.close()

@st.cache_data(ttl=30)
def get_seen_houses():
    """Get seen houses from local database."""
    db = HousingDataDB()
    try:
        seen_houses = db.get_seen_houses()
        return seen_houses
    finally:
        db.close()

def add_seen_house(ou_id: int):
    """Add house to seen list."""
    db = HousingDataDB()
    try:
        db.add_seen_house(ou_id)
    finally:
        db.close()

# Load data
try:
    listings_scored = get_listings_data()
    seen_houses = get_seen_houses()
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

if listings_scored.empty:
    st.warning("No data available. Please run the data pipeline first using `python run_pipeline.py`")
    st.stop()

# Main app content
st.title("🏠 Boligoversigt - Lokal Version")

# Display scoring information
with st.expander("ℹ️ Information om scoring systemet"):
    st.write("**Scoring Algoritme - Samlet score (Max: 80 point)**")
    st.write("Hvert hus får en score baseret på 8 forskellige faktorer. Hver faktor gives en score fra 0-10 point, som tilsammen giver en maksimal score på 80 point.")
    st.write("• **Energiklasse**: Bedre energiklasse = højere score (A=10, B=8, C=6, osv.)")
    st.write("• **Togstation afstand**: Jo tættere på togstation, jo højere score")
    st.write("• **Grundstørrelse**: Større grund = højere score sammenlignet med andre i samme område")
    st.write("• **Husstørrelse**: Større hus = højere score sammenlignet med andre i samme område")
    st.write("• **Pris effektivitet**: Lavere **pris per m²** = højere score sammenlignet med andre i samme område")
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
if not seen_houses.empty:
    seen_houses['is_seen'] = True
    listings_scored = listings_scored.merge(seen_houses[['ouId', 'is_seen']], on='ouId', how='left')
    listings_scored['is_seen'] = listings_scored['is_seen'].fillna(False)
else:
    # If no seen houses, create the column with all False values
    listings_scored['is_seen'] = False

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
    data_tab, scores_tab, seen_tab = st.tabs(["🏠 Boliger", "📊 Pointdetaljer", "🔍 Allerede sete huse"])
    
    with data_tab:
        st.info("💡 **Vigtigt**: Pris-scoren baseres på **pris per m²**, ikke samlet pris. Dette betyder at et stort hus med lav m²-pris kan score højere end et lille hus med høj m²-pris.")
        
        # Main property information
        if not filtered_listings.empty:
            property_columns = ['full_address', 'price', 'm2_price', 'm2', 'rooms', 'built', 
                              'energy_class', 'lot_size', 'basement_size', 'days_on_market', 'total_score']
            
            st.dataframe(
                data=filtered_listings[property_columns], 
                height=600, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "total_score": st.column_config.NumberColumn("Samlet score", format="%.1f"),
                    "price": st.column_config.NumberColumn("Samlet pris", format="%d"),
                    "m2_price": st.column_config.NumberColumn("Pris/m² (scored)", format="%d"),
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

            # Add to seen houses functionality
            st.subheader("Markér huse som sete")
            ouid_input = st.text_input("Indtast ouID'er adskilt af kommaer")
            if st.button("Tilføj til sete huse"):
                ouids = [ouid.strip() for ouid in ouid_input.split(",") if ouid.strip()]
                if ouids:
                    try:
                        for ouid in ouids:
                            add_seen_house(int(ouid))
                        st.success("ouID'er tilføjet til sete huse")
                        # Clear cache to refresh data
                        st.cache_data.clear()
                        st.rerun()
                    except ValueError:
                        st.error("Indtast venligst gyldige numeriske ouID'er")
                    except Exception as e:
                        st.error(f"Fejl ved tilføjelse: {e}")
                else:
                    st.error("Indtast venligst gyldige ouID'er")
        else:
            st.warning("Ingen boliger matcher dine filterkriterier.")
    
    with scores_tab:
        if not filtered_listings.empty:
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
                    "score_price_efficiency": st.column_config.NumberColumn("Pris/m²", format="%.1f"),
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
        else:
            st.warning("Ingen boliger matcher dine filterkriterier.")
    
    with seen_tab:
        # Seen Houses section
        st.subheader("🔍 Sete Huse")
        
        if not seen_houses.empty:
            # Join seen houses with listings for addresses
            seen_with_addresses = seen_houses.merge(
                listings_scored[['ouId', 'full_address']], 
                on='ouId', 
                how='left'
            )
            st.dataframe(
                data=seen_with_addresses[['full_address', 'seen_at']], 
                height=400, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "full_address": st.column_config.TextColumn("Adresse"),
                    "seen_at": st.column_config.DatetimeColumn("Set dato")
                }
            )
        else:
            st.info("Ingen huse er markeret som sete endnu.")
else:
    st.write("Vælg venligst mindst ét postnummer.")

# Add refresh data button
if st.sidebar.button("🔄 Genindlæs data"):
    st.cache_data.clear()
    st.rerun()

# Show data freshness
try:
    db = HousingDataDB()
    last_update = db.conn.execute("SELECT MAX(loaded_at_utc) as last_update FROM listings").fetchone()[0]
    if last_update:
        st.sidebar.info(f"Data sidst opdateret: {last_update}")
    db.close()
except Exception:
    pass
