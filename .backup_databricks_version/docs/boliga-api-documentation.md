# Boliga.dk API Dokumentation

## 📋 Oversigt
Denne fil dokumenterer strukturen og tilgængelige felter fra boliga.dk's interne API, som vi scraper via deres JavaScript app state.

## 🔗 URL Struktur
```
https://www.boliga.dk/resultat?zipCodes={zip_code}&propertyType={property_type}&page={page}
```

### Parametre:
- `zipCodes`: Kommasepareret liste af postnumre (f.eks. "8000,8200")
- `propertyType`: 1=Hus, 2=Rækkehus, 3=Ejerlejlighed, etc.
- `page`: Side nummer (starter fra 1)

## 📊 JSON Struktur

### Lokation i HTML
Data findes i: `<script id="boliga-app-state">{JSON_DATA}</script>`

### Encoding
- JSON bruger `&q;` i stedet for `"`
- Skal konverteres: `json_str.replace('&q;', '"')`

### Data Hierarki
```
{
  "search-service-perform": {
    "meta": {
      "searchGuid": "string",
      "total": number
    },
    "results": [
      {
        // Listing objekter (se nedenfor)
      }
    ],
    "randomTypeHuse": {
      "houses": [...],
      "leisureHouses": [...]
    }
  }
}
```

## 🏠 Listing Objekt Struktur

### Basis Felter (100% tilgængelige)
```javascript
{
  "id": 2237536,                    // Intern listing ID
  "ouId": 1691948255,               // Unique property identifier
  "street": "Kaserneboulevarden 25", // Fuld adresse
  "city": "Aarhus C",               // By navn
  "zipCode": 8000,                  // Postnummer
  "price": 10995000,                // Pris i DKK
  "rooms": 6,                       // Antal værelser
  "size": 216,                      // Størrelse i m²
  "buildYear": 1893,                // Byggeår
  "squaremeterPrice": 50902,        // Pris per m²
  "daysForSale": 9,                 // Dage på markedet
  "propertyType": 1                 // Boligtype (1=hus)
}
```

### GPS & Lokation (100% tilgængelige)
```javascript
{
  "latitude": 56.16455,             // GPS breddegrad
  "longitude": 10.19815,            // GPS længdegrad
  "municipality": 751,              // Kommune kode
  "area": 10                        // Område kode
}
```

### Energi & Størrelse (80-100% tilgængelige)
```javascript
{
  "energyClass": "d",               // Energimærke (a-g, ofte lowercase)
  "lotSize": 810,                   // Grundstørrelse i m²
  "basementSize": 116,              // Kælderstørrelse i m² (80% coverage)
  "floor": null                     // Etage (mest null for huse)
}
```

### Markedsdata (variabel tilgængelighed)
```javascript
{
  "priceChangePercentTotal": 0,     // Total prisændring i % (10% coverage)
  "evaluationPrice": 0,             // Vurderingspris (ofte 0)
  "net": 0,                         // Net værdi (ofte 0)
  "exp": 2131                       // Ekspeditionsgebyr
}
```

### Status & Flags
```javascript
{
  "isActive": true,                 // Om listing er aktiv
  "isForeclosure": false,           // Tvangsauktion
  "selfsale": false,                // Privat salg
  "isPremiumAgent": false,          // Premium mægler
  "boligaPlus": false,              // Boliga Plus listing
  "inWatchlist": false,             // I bruger's watchlist
  "onTheWay": false,                // "På vej" status
  "useOuFlag": false                // Intern flag
}
```

### Visning & Marketing
```javascript
{
  "openHouse": "",                  // Åbent hus info (ofte tom)
  "views": 777,                     // Antal visninger
  "showLogo": false,                // Vis mægler logo
  "nonPremiumDiscrete": false,      // Diskret listing flag
  "randomTypeHuse": null            // Tilfældig hus type
}
```

### Billeder
```javascript
{
  "images": [
    {
      "id": 2237536,
      "date": "2025-06-30T18:28:37.000Z",
      "url": "https://i.boliga.org/dk/500x/2237/2237536.jpg"
    }
  ]
}
```

### Mægler Information
```javascript
{
  "agentRegId": 530,                // Mægler registrerings ID
  "agentDisplayName": "Nybolig...", // Mægler navn
  "domainId": 9,                    // Domain ID
  "businessArea": 0                 // Forretnings område
}
```

### Tekniske Felter
```javascript
{
  "guid": "9DFE15BB-F862-4545-A59E-DC387D585923",
  "itemType": 0,
  "groupKey": null,
  "bfeNr": 1452322,
  "ouAddress": "kaserneboulevarden-25-8000-aarhus-c",
  "cleanStreet": "Kaserneboulevarden",
  "otwAddress": null,
  "dsAddress": null,
  "adresseId": "0A3F50BF-F969-32B8-E044-0003BA298018",
  "dawaId": null,
  "projectSaleUrl": null,
  "additionalBuildings": null
}
```

### Datoer
```javascript
{
  "createdDate": "2023-10-13T23:59:51.503Z",  // Oprettelses dato
  "lastSeen": "2025-06-29T23:49:42.463Z",     // Sidst set
  "lastSoldDate": null                        // Sidst solgt (ofte null)
}
```

### Finansiering
```javascript
{
  "downPayment": 75000              // Udbetaling
}
```

## 📈 Data Kvalitet

Baseret på analyse af 10 listings fra Aarhus C (8000):

| Felt | Coverage | Kommentar |
|------|----------|-----------|
| Basis felter | 100% | Altid tilstede |
| GPS koordinater | 100% | Meget nøjagtige |
| Energimærke | 100% | Ofte lowercase |
| Grundstørrelse | 100% | Pålidelige data |
| Kælderstørrelse | 80% | Ikke alle huse har kælder |
| Prisændring | 10% | Sjældent udfyldt |
| Åbent hus | <5% | Mest tomme |
| Billeder | 90% | 1-10 billeder per listing |

## 🔧 Implementation Noter

### Robust Parsing
```python
# Safe extraction med fallbacks
latitude = float(listing.get('latitude', 0.0))
energy_class = listing.get('energyClass', '').lower()
lot_size = float(listing.get('lotSize', 0))
```

### Error Handling
```python
try:
    # Parse individual listing
    pass
except (KeyError, ValueError, TypeError) as e:
    logging.warning(f'Error parsing listing: {e}')
    continue  # Skip problematic listings
```

### Performance Tips
- Cache requests når muligt
- Batch process multiple zip codes
- Handle rate limiting gracefully
- Parse JSON én gang per request

## 🚫 Begrænsninger

1. **Rate Limiting**: Boliga.dk kan implementere rate limiting
2. **Schema Changes**: API struktur kan ændre sig uden varsel  
3. **Data Kvalitet**: Ikke alle felter er altid udfyldt
4. **Legal**: Respekter terms of service og robots.txt

## 📝 Changelog

### 2025-06-30
- Initial dokumentation
- Identificeret alle tilgængelige felter
- Dokumenteret data kvalitet og coverage
