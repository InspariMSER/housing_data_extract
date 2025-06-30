# Housing Data Extract - Hus-søgningssystem

Dette projekt skal hjælpe mig og min kæreste med at finde det perfekte hus i Aarhus-området. Systemet scraper boligdata fra boliga.dk, beregner en score baseret på vores præferencer, og præsenterer resultaterne gennem en interaktiv webapp.

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

## 📊 Nuværende scoring algoritme

Hver bolig scores på 5 parametre (max 10 point hver):
- **Byggeår**: Nyere = højere score
- **Dage på markedet**: Færre dage = højere score  
- **m²**: Større = højere score
- **Pris**: Lavere = højere score
- **Værelser**: Færre = højere score

**Total max score**: 50 point

## 🚀 Planlagte forbedringer

### Nye datafelter at udnytte:
- `energyClass`: Energimærke (A-G) - vigtigt for driftsomkostninger
- `lotSize`: Grundstørrelse - vigtigt for have og udvidelser
- `latitude`/`longitude`: GPS koordinater for afstandsberegning til togstationer
- `priceChangePercentTotal`: Prisudvikling - indikator for markedstendens
- `isForeclosure`: Tvangsauktion flag - bør undgås
- `basementSize`: Kælderstørrelse - ekstra værdi
- `openHouse`: Åbent hus information
- `images`: Links til boligbilleder

### Forbedret scoring algoritme:
1. **Energimærke** (høj vægt): A=10, B=8, C=6, D=4, E=2, F/G=0
2. **Afstand til tog** (høj vægt): Beregnet via GPS koordinater
3. **Grundstørrelse** (medium vægt): Større grund = flere point
4. **Prisudvikling** (medium vægt): Faldende/stabile priser = flere point
5. **Byggeår** (bibeholdt vægt)
6. **m²/pris ratio** (justeret vægt)
7. **Tvangsauktion filter**: Automatisk ekskludering

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
2. **Implementer forbedret scoring** med energimærke og afstand til tog - **I GANG**
3. **Migrer til DuckDB** og pandas-baseret processing
4. **Sæt notifikationssystem op** med email alerts
5. **Optimér performance** og reducer kompleksitet

## 📚 Teknisk Dokumentation

- [`extraction-update-log.md`](extraction-update-log.md): Detaljeret log over opdateringer til data extraction
- [`boliga-api-documentation.md`](boliga-api-documentation.md): Komplet dokumentation af boliga.dk's API struktur

## 📍 Målområder (postnumre)

Nuværende fokus på 41 postnumre omkring Aarhus med togforbindelse:
8000-8382, 8400-8471, 8520-8550, 8600, 8660, 8680, 8850-8900