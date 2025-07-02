# Local Housing Data Extract Setup

## Prerequisites

- Python 3.8+
- Internet connection for scraping boliga.dk

## Installation

1. **Clone/navigate to the project directory:**
   ```bash
   cd /home/mser/code/repos/housing_data_extract
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Create necessary directories:**
   ```bash
   mkdir -p data logs
   ```

## Running the System

### 🎯 Unified Startup Script (ANBEFALET)
Brug det nye `start.py` script til alle operationer:

**Første gang - kør pipeline og start webapp:**
```bash
python start.py full
```

**Kør kun data pipeline:**
```bash
python start.py pipeline
```

**Start kun webapp:**
```bash
python start.py app
```

**Start automated scheduler:**
```bash
python start.py scheduler
```

### 📖 Detaljerede kommandoer

#### 1. Manual Pipeline Run
Extract and process housing data once:
```bash
python start.py pipeline
# ELLER direkte:
python pipeline/run_pipeline.py
```

#### 2. Run the Web App
Start the Streamlit app to browse housing data:
```bash
python start.py app
# ELLER direkte:
streamlit run app/app_local.py
```
The app will be available at http://localhost:8501

#### 3. Scheduled Automatic Runs
Set up automatic data collection:

**Run automated scheduler:**
```bash
python start.py scheduler
# ELLER direkte:
python scripts/scheduler.py
```
```bash
python scheduler.py --run-once
```

**Start the scheduler (runs daily at 6 AM + every 4 hours during active hours):**
```bash
python scheduler.py
```

## File Structure

```
housing_data_extract/
├── src/
│   ├── extract_listings_local.py   # Data extraction from boliga.dk
│   ├── transform_listings_local.py # Data transformation and scoring
│   └── database_local.py           # DuckDB database management
├── data/
│   └── housing.duckdb              # Local database file (auto-created)
├── logs/                           # Application logs
├── run_pipeline.py                 # Main pipeline orchestrator
├── app_local.py                    # Streamlit web application
├── scheduler.py                    # Automated scheduling
└── requirements.txt                # Python dependencies
```

## Architecture Migration

**From Databricks:**
- ✅ Databricks Delta Lake → DuckDB
- ✅ PySpark DataFrame → Pandas DataFrame  
- ✅ Databricks SQL → DuckDB SQL
- ✅ Databricks Apps → Local Streamlit
- ✅ Databricks Jobs → APScheduler

**Benefits of Local Setup:**
- No cloud costs
- Full data control
- Faster development iteration
- No internet dependency for app usage
- Simple backup and restore

## Database

The system uses DuckDB, a fast analytical database that stores data in a single file:
- **Location:** `data/housing.duckdb`
- **Tables:**
  - `listings`: Raw scraped data
  - `listings_scored`: Processed data with scores
  - `seen_houses`: User-marked houses

## Scoring Algorithm

The enhanced 8-factor scoring system (max 80 points):

1. **Energy Class** (0-10 pts): A=10, B=8, C=6, D=4, E=2, F=0, Unknown=3
2. **Train Distance** (0-10 pts): GPS distance to nearest train/light rail
3. **Lot Size** (0-10 pts): Relative to other houses in same zip code
4. **House Size** (0-10 pts): Relative to other houses in same zip code  
5. **Price Efficiency** (0-10 pts): Price/m² relative to same zip code
6. **Build Year** (0-10 pts): Relative to other houses in same zip code
7. **Basement Size** (0-10 pts): Relative to other houses in same zip code
8. **Days on Market** (0-10 pts): Relative to other houses in same zip code

## Monitoring

Check logs for system health:
```bash
# Pipeline logs
tail -f logs/housing_pipeline.log

# Scheduler logs  
tail -f logs/scheduler.log
```

## Troubleshooting

**No data in app:**
1. Run the pipeline manually: `python run_pipeline.py`
2. Check logs for errors
3. Verify internet connection

**Database issues:**
1. Delete `data/housing.duckdb` to reset
2. Re-run pipeline to rebuild data

**Streamlit errors:**
1. Restart the app: `Ctrl+C` then `streamlit run app_local.py`
2. Check if pipeline has run successfully

## Systemd Service (Optional)

To run the scheduler as a system service:

1. Create service file: `/etc/systemd/system/housing-data.service`
2. Enable and start: `sudo systemctl enable housing-data && sudo systemctl start housing-data`

This ensures the system continues running after reboots.
