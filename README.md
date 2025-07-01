# Housing Data Extract - Hus-søgningssystem

Dette projekt skal hjælpe mig og min kæreste med at finde det## 📚 Teknisk Dokumentation

- [`extraction-update-log.md`](docs/extraction-update-log.md): Detaljeret log over opdateringer til data extraction
- [`boliga-api-documentation.md`](docs/boliga-api-documentation.md): Komplet dokumentation af boliga.dk's API strukturfekte hus i Aarhus-området. Systemet scraper boligdata fra boliga.dk, beregner en score baseret på vores præferencer, og præsenterer resultaterne gennem en interaktiv webapp.

## 🎯 Projektmål

- **Geografisk fokus**: Kun områder med togforbindelse til Aarhus Banegård (almindelig tog eller letbane)
- **Boligtype**: Kun huse (ikke lejligheder, rækkehuse, etc.)
- **Automatisering**: Daglig scraping og scoring af nye boliger
- **Notifikationer**: Advarsler ved interessante nye boliger
- **Målplatform**: Migrere fra Databricks til lokal kørsel

## 🏗️ Nuværende arkitektur (Databricks)

### Data Pipeline
1. **Extract** (`extract/`): Scraper boliga.dk for 41 postnumre omkring Aarhus
2. **Transform** (`transform/`): Beregner score baseret på byggeår, pris, størrelse, værelser og dage på markedet
3. **App** (`app/`): Streamlit webapp til browsing og marking af sete huse

### Teknologi Stack
- **Database**: Databricks Delta Lake
- **Processing**: PySpark
- **Frontend**: Streamlit
- **Deployment**: Databricks Apps

## 🔄 Planlagt migrering til lokal kørsel

### Målarkitektur
- **Database**: DuckDB (let og hurtig)
- **Processing**: Pandas (simplere end Spark)
- **Scheduling**: Python APScheduler + systemd service
- **Notifikationer**: Gmail SMTP (gratis)
- **Frontend**: Streamlit på localhost (lokalt deployment)
- **Hosting**: Selvstændig server/computer i stedet for cloud platform

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
1. **Data extraction modernisering** - Alle nye felter ekstrakteret og valideret
2. **Forbedret scoring algoritme** - Implementeret og integreret i pipeline
3. **Streamlit app forbedringer** - Nye filtre og score breakdown visning

### 🔄 I gang:

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
3. **Migrer til DuckDB** og pandas-baseret processing - **NÆSTE OPGAVE**
4. **Sæt notifikationssystem op** med email alerts
5. **Optimér performance** og reducer kompleksitet

## 📚 Teknisk Dokumentation

- [`docs/extraction-update-log.md`](docs/extraction-update-log.md): Detaljeret log over opdateringer til data extraction
- [`docs/boliga-api-documentation.md`](docs/boliga-api-documentation.md): Komplet dokumentation af boliga.dk's API struktur
- [`docs/enhanced-scoring-algorithm.md`](docs/enhanced-scoring-algorithm.md): Detaljeret dokumentation af den forbedrede scoring algoritme

## 📍 Målområder (postnumre)

Nuværende fokus på 41 postnumre omkring Aarhus med togforbindelse:
8000-8382, 8400-8471, 8520-8550, 8600, 8660, 8680, 8850-8900