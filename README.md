# Housing Data Extract - Hus-søgningssystem

Dette projekt hjælper med at finde det perfekte hus i Aarhus-området. Systemet scraper boligdata fra boliga.dk, beregner en score baseret på vores præferencer, og præsenterer resultaterne gennem en interaktiv webapp.

## 🎯 Projektmål

- **Geografisk fokus**: Kun områder med togforbindelse til Aarhus Banegård (almindelig tog eller letbane)
- **Boligtype**: Kun huse (ikke lejligheder, rækkehuse, etc.)
- **Automatisering**: Daglig scraping og scoring af nye boliger
- **Notifikationer**: Advarsler ved interessante nye boliger
- **Platform**: Lokal kørsel med DuckDB og Pandas

## 🏗️ Arkitektur (Lokal)

### Data Pipeline
1. **Extract** (`src/extract_listings_local.py`): Scraper boliga.dk for postnumre omkring Aarhus
2. **Transform** (`src/transform_listings_local.py`): Beregner score baseret på byggeår, pris, størrelse, værelser og dage på markedet
3. **App** (`app/app_local.py`): Streamlit webapp til browsing og marking af sete huse
4. **Database** (`src/database_local.py`): DuckDB database management
5. **Scheduler** (`scripts/scheduler.py`): Automated daily pipeline execution
6. **Pipeline Runner** (`pipeline/run_pipeline.py`): Main pipeline orchestration

### Teknologi Stack
- **Database**: DuckDB (let og hurtig lokal database)
- **Processing**: Pandas (dataframe processing)
- **Scheduling**: APScheduler (Python scheduling)
- **Frontend**: Streamlit (lokal webapp)
- **Environment**: Python virtual environment

## 🚀 Quick Start

### Kort opsætning:
```bash
# Clone repository
git clone <repo-url>
cd housing_data_extract

# Setup virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt

# Kør pipeline og start webapp (anbefalet første gang)
python start.py full

# Eller kør kun pipeline
python start.py pipeline

# Eller start kun webapp
python start.py app

# Eller start automated scheduler
python start.py scheduler
```

Se [`LOCAL_SETUP.md`](LOCAL_SETUP.md) for detaljeret setup guide.

## 📊 Forbedret scoring algoritme (OPDATERET!)

Hver bolig scores nu på **8 parametre** med equal weighting (max 80 point):

**Globale faktorer (samme for alle huse):**
- **Energiklasse** (10 point max): A=10, B=8, C=6, D=4, E=2, F/G=0, UNKNOWN=3
- **Afstand til tog** (10 point max): Beregnet via GPS koordinater til S-tog og letbane

**Relative faktorer (sammenlignet inden for samme postnummer):**
- **Grundstørrelse** (10 point max): Større grund = højere score relativt til området
- **Husstørrelse** (10 point max): Større hus = højere score relativt til området  
- **Priseffektivitet** (10 point max): Lavere m²-pris = højere score relativt til området
- **Byggeår** (10 point max): Nyere hus = højere score relativt til området
- **Kælderareal** (10 point max): Større kælder = højere score relativt til området
- **Dage på marked** (10 point max): Færre dage = højere score relativt til området

**Total max score**: 80 point

**Energimærke håndtering**: 
- Boliga.dk har mærkelige værdier som G,H,I,J,K,L der faktisk er A-klasse
- '-' eller manglende værdier bliver til UNKNOWN (3 point)
- Alle værdier normaliseres til store bogstaver

## 🚀 Status opdatering

### ✅ Completeret:
1. **Migrering til lokal kørsel** - DuckDB og Pandas pipeline implementeret og testet
2. **Data extraction modernisering** - Alle nye felter ekstrakteret og valideret
3. **Forbedret scoring algoritme** - Implementeret og integreret i pipeline
4. **Streamlit app forbedringer** - Lokal version med nye filtre og score breakdown
5. **Repository cleanup** - Fjernet alle legacy Databricks filer og scripts

### 🔄 I gang:
- Performance optimering af scraping
- Email notifikationssystem
- Automatisk deployment til produktionsserver

## 📋 Data eksempel fra boliga.dk

```json
{
  "id": 2041515,
  "latitude": 56.31307,
  "longitude": 10.04435,
  "propertyType": 1,
  "priceChangePercentTotal": -9,
  "energyClass": "C",
  "price": 1450000,
  "rooms": 6,
  "size": 182,
  "lotSize": 1532,
  "buildYear": 1957,
  "city": "Hadsten",
  "isForeclosure": false,
  "zipCode": 8370,
  "street": "Skanderborgvej 16",
  "squaremeterPrice": 7967,
  "daysForSale": 625,
  "basementSize": 30,
  "images": [
    {
      "id": 2041515,
      "url": "https://i.boliga.org/dk/500x/2041/2041515.jpg"
    }
  ]
}
```

## 🎯 Næste skridt

1. ✅ **Opdater data extraction** til at inkludere alle relevante felter - **FULDFØRT**
2. ✅ **Implementer forbedret scoring** med energimærke og afstand til tog - **FULDFØRT**
3. ✅ **Migrer til DuckDB** og pandas-baseret processing - **FULDFØRT**
4. **Sæt notifikationssystem op** med email alerts
5. **Implementer automatisk scheduling** på produktionsserver
6. **Performance optimering** og memory usage forbedringer

## 📁 Projekt struktur

```
housing_data_extract/
├── start.py                      # 🎯 MAIN STARTUP SCRIPT
├── README.md                     # Dette dokument
├── LOCAL_SETUP.md               # Detaljeret setup guide
├── requirements.txt             # Python dependencies
│
├── app/                         # 🖥️ Streamlit webapp
│   ├── __init__.py
│   └── app_local.py            # Streamlit interface
│
├── pipeline/                    # ⚙️ Data pipeline
│   ├── __init__.py
│   └── run_pipeline.py         # Pipeline orchestration
│
├── scripts/                     # 📜 Automation scripts
│   ├── __init__.py
│   └── scheduler.py            # Automated scheduling
│
├── src/                        # 🔧 Core modules
│   ├── __init__.py
│   ├── extract_listings_local.py   # Data extraction
│   ├── transform_listings_local.py # Data transformation & scoring
│   └── database_local.py           # DuckDB management
│
├── data/                       # 💾 Generated data
│   └── housing.duckdb         # Local database (auto-generated)
│
├── logs/                       # 📝 Application logs (auto-generated)
├── docs/                       # 📚 Technical documentation
└── venv/                       # 🐍 Python virtual environment
```

### 🎯 Hovedkommandoer:
- **`python start.py full`** - Kør pipeline + start webapp (anbefalet første gang)
- **`python start.py pipeline`** - Kør kun data pipeline
- **`python start.py app`** - Start kun webapp
- **`python start.py scheduler`** - Start automated scheduler

## 📚 Teknisk Dokumentation

- [`docs/extraction-update-log.md`](docs/extraction-update-log.md): Detaljeret log over opdateringer til data extraction
- [`docs/boliga-api-documentation.md`](docs/boliga-api-documentation.md): Komplet dokumentation af boliga.dk's API struktur
- [`docs/enhanced-scoring-algorithm.md`](docs/enhanced-scoring-algorithm.md): Detaljeret dokumentation af den forbedrede scoring algoritme

## 📍 Målområder (postnumre)

Nuværende fokus på 41 postnumre omkring Aarhus med togforbindelse:
8000-8382, 8400-8471, 8520-8550, 8600, 8660, 8680, 8850-8900