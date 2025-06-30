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
    # Query data from the specified tables
    listings_scored = sqlQuery("select full_address, price, m2, m2_price, rooms, cast(built as string) as built, zip_code, days_on_market, is_in_zip_code_city, total_score, ouID from mser_catalog.housing.listings_scored")
    return listings_scored

def get_seen_houses():
    seen_houses = sqlQuery("select ouID from mser_catalog.housing.seen_houses")
    return seen_houses

listings_scored = get_listings_Data()
seen_houses = get_seen_houses()

# Create tabs for listings and seen houses
tab1, tab2 = st.tabs(["Listings", "Seen Houses"])

with tab1:
    st.title("Listings")
    # Create a filter on the right side of the screen for zip_codes
    zip_codes = sorted(listings_scored['zip_code'].unique())
    show_city_in_zip = st.sidebar.checkbox("Show only listings with city in Zip Code", value=True)
    show_already_seen_houses = st.sidebar.checkbox("Show houses already marked as seen", value=False)
    selected_zip_codes = st.sidebar.multiselect("Select Zip Codes", zip_codes, default=zip_codes)
    max_budget = st.sidebar.number_input("Maximum Budget", min_value=0, max_value=15000000, value=4000000)
    min_rooms = st.sidebar.number_input("Minimum Rooms", min_value=0, max_value=15, value=5)
    min_m2 = st.sidebar.number_input("Minimum M2", min_value=0, max_value=10000, value=150)
    min_score = st.sidebar.number_input("Minimum Total Score", min_value=0, max_value=40, value= 0)

    # Join the seen_houses onto the listings_scored dataframe
    listings_scored = listings_scored.merge(seen_houses.assign(is_seen=True), on='ouID', how='left').fillna({'is_seen': False})

    # Filter dataframe based on seen_houses
    if not show_already_seen_houses:
        filtered_listings = listings_scored[~listings_scored['is_seen']]
    else:
        filtered_listings = listings_scored      

    # Filter the dataframe based on the selected zip codes
    filtered_listings = filtered_listings[filtered_listings['zip_code'].isin(selected_zip_codes)]
    filtered_listings = filtered_listings[filtered_listings['price'] <= max_budget]
    filtered_listings = filtered_listings[filtered_listings['rooms'] >= min_rooms]
    filtered_listings = filtered_listings[filtered_listings['m2'] >= min_m2]
    filtered_listings = filtered_listings[filtered_listings['total_score'] >= min_score]


    if show_city_in_zip:
        filtered_listings = filtered_listings[filtered_listings['is_in_zip_code_city'] == show_city_in_zip]
    filtered_listings = filtered_listings.sort_values(by='total_score', ascending=False)

    if selected_zip_codes:  
        st.dataframe(data=filtered_listings, height=600, use_container_width=True, hide_index=True)
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

with tab2:
    st.title("Seen Houses")
    sqlquery = f"""select b.full_address from mser_catalog.housing.seen_houses as a INNER JOIN mser_catalog.housing.listings_scored as b on a.ouID=b.ouID"""
    houses_joined = sqlQuery(sqlquery)
    if st.button("Refresh Data"):
        houses_joined = sqlQuery(sqlquery)
    st.dataframe(data=houses_joined, height=600, use_container_width=True, hide_index=True)