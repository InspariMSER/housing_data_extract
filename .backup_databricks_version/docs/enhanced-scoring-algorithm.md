# Enhanced Scoring Algorithm Documentation

## Overview

The enhanced scoring algorithm evaluates properties based on 8 factors with equal weighting, providing a maximum score of 80 points. All factors except energy class and train distance are scored relative to other properties within the same zip code.

## Scoring Method

### Global Scoring (2 factors)
Applied consistently across all zip codes:
- **Energy Class**: Universal energy efficiency standards
- **Train Distance**: Geographic proximity to transit

### Relative Scoring (6 factors) 
Ranked within each zip code using dense ranking:
- **Build Year**: Compared to other houses in same zip code
- **House Size**: Relative to local market
- **Price Efficiency**: Compared to local price/m² values
- **Lot Size**: Relative to local lot sizes
- **Basement Size**: Compared to local basement sizes  
- **Days on Market**: Relative to local market timing

## Scoring Factors (All 10 points max except noted)

### 1. Energy Class Score (Global)
- **A**: 10 points
- **B**: 8 points  
- **C**: 6 points
- **D**: 4 points
- **E**: 2 points
- **F/G**: 0 points
- **Missing/Unknown**: 3 points (default)

### 2. Train Distance Score (Global)
- Calculated using Haversine formula to nearest train station or light rail stop
- Stations included:
  - **Main Stations**: Aarhus H, Skanderborg St, Randers St, Hadsten St, Hinnerup St, Langå St
  - **Light Rail**: Risskov, Skejby, Universitetshospitalet, Skejby Sygehus, Lisbjerg Skole, Lisbjerg Kirkeby, Lystrup, Ryomgård, Grenaa
- Score formula: `10 * (1 - distance_km / 25)`
- Maximum distance considered: 25km
- All stations have equal weight (no station weighting)

### 3. Build Year Score (Relative within zip code)
- Uses dense ranking: newest house in zip code = 10 points, oldest = 0 points
- Linear interpolation between ranks

### 4. House Size Score (Relative within zip code)
- Uses dense ranking: largest house in zip code = 10 points, smallest = 0 points
- Based on m² floor area

### 5. Price Efficiency Score (Relative within zip code)
- Uses dense ranking based on price per m²: cheapest/m² in zip code = 10 points, most expensive = 0 points
- Calculated as: price ÷ m² then ranked

### 6. Lot Size Score (Relative within zip code)
- Uses dense ranking: largest lot in zip code = 10 points, smallest = 0 points
- Based on lot size in m²

### 7. Basement Size Score (10 points max, Relative within zip code)
- Uses dense ranking: largest basement in zip code = 10 points, smallest = 0 points
- Based on basement size in m²

### 8. Days on Market Score (Relative within zip code)
- Uses dense ranking: fewest days in zip code = 10 points, most days = 0 points
- Fresher listings score higher

## Total Score Calculation

```
total_score = energy_score + 
              train_distance_score + 
              build_year_score + 
              house_size_score + 
              price_efficiency_score + 
              lot_size_score + 
              basement_score + 
              days_market_score
```

**Maximum possible score**: 80 points (8 × 10 points)

## Implementation Details

### Relative Scoring Formula
For zip code relative factors:
```
score = 10 * (rank - 1) / (max_rank - 1)
```
Where rank 1 = best value, max_rank = worst value in that zip code.

### UDF Functions
- `calculate_distance_udf()`: Calculates train station distances using Haversine formula
- `energy_class_score_udf()`: Maps energy classes to scores with safe fallbacks

### Error Handling
- Missing GPS coordinates default to 0 points for train distance
- Missing energy class defaults to 3 points
- Missing lot size/basement size default to 0
- Safe type casting with fallbacks
- Single house in zip code gets 10 points for all relative factors

### Performance Considerations
- Window functions partitioned by zip_code for relative scoring
- UDFs registered once per session
- Final score rounded to 2 decimal places

## Database Schema

The algorithm outputs these columns:
- `total_score`: Total points (0-80)
- `score_energy`: Energy class points (0-10)
- `score_train_distance`: Train proximity points (0-10)
- `score_build_year`: Build year points (0-10)
- `score_house_size`: House size points (0-10)
- `score_price_efficiency`: Price efficiency points (0-10)
- `score_lot_size`: Lot size points (0-10)
- `score_basement`: Basement points (0-10)
- `score_days_market`: Market timing points (0-10)

## Advantages of This Approach

### Fair Comparison
- Houses compete within their local market context
- A 1960s house in Aarhus C isn't penalized against 2020s houses in suburbs
- Price efficiency reflects local market realities

### Balanced Scoring
- All factors have equal 10-point contribution
- No artificial weighting that might skew results
- Simple and transparent scoring system

### Market Relevant
- Relative scoring reflects what buyers actually compare
- Accounts for zip code characteristics and market segments
- Energy and transport remain universally comparable

## Changes from Previous Version

### Removed Elements
- Weighted scoring (all factors now equal)
- Fixed categorical scoring for most factors
- Station weighting for train proximity

### Enhanced Elements
- Full zip code relative scoring for 6/8 factors
- Expanded light rail station coverage
- Basement scoring increased to 10 points (from 5)
- Added Langå station for northern coverage

### Technical Improvements
- Better error handling for edge cases
- Cleaner code structure with utils.py
- More comprehensive station coverage

#### 3. Lot Size Score (10 points max, weight: 1.0x)
- 0-500m²: 0-5 points (linear)
- 500-1500m²: 5-10 points (linear)
- >1500m²: 10 points (capped)

#### 4. House Size Score (10 points max, weight: 1.0x)
- ≥200m²: 10 points (Very large)
- ≥150m²: 8 points (Large)
- ≥120m²: 6 points (Good size)
- ≥100m²: 4 points (Decent)
- ≥80m²: 2 points (Small)
- <80m²: 1 point (Very small)

#### 5. Price Efficiency Score (10 points max, weight: 1.0x)
Based on price per m²:
- ≤25,000 DKK/m²: 10 points (Very good value)
- ≤35,000 DKK/m²: 8 points (Good value)
- ≤45,000 DKK/m²: 6 points (Fair value)
- ≤55,000 DKK/m²: 4 points (Expensive)
- ≤70,000 DKK/m²: 2 points (Very expensive)
- >70,000 DKK/m²: 1 point (Extremely expensive)

#### 6. Build Year Score (8 points max, weight: 0.8x)
- ≥2010: 10 points (Very new)
- ≥2000: 8 points (New)
- ≥1990: 6 points (Modern)
- ≥1980: 4 points (Decent)
- ≥1960: 2 points (Older)
- <1960: 1 point (Very old)

### Low Weight Factors (5.5 points max)

#### 7. Basement Size Score (2.5 points max, weight: 0.5x)
- 0-50m²: 0-2 points (linear)
- 50-100m²: 2-5 points (linear)
- >100m²: 5 points (capped)

#### 8. Days on Market Score (3 points max, weight: 0.3x)
- ≤7 days: 10 points (Very fresh)
- ≤30 days: 8 points (Fresh)
- ≤90 days: 6 points (Normal)
- ≤180 days: 4 points (Getting stale)
- ≤365 days: 2 points (Stale)
- >365 days: 1 point (Very stale)

## Total Score Calculation

```
total_score = (energy_score * 1.5) + 
              (train_distance_score * 1.5) + 
              (lot_size_score * 1.0) + 
              (house_size_score * 1.0) + 
              (price_efficiency_score * 1.0) + 
              (build_year_score * 0.8) + 
              (basement_score * 0.5) + 
              (days_market_score * 0.3)
```

**Maximum possible score**: 73.5 points

## Implementation Details

### UDF Functions
- `calculate_distance_udf()`: Calculates train station distances using Haversine formula
- `energy_class_score_udf()`: Maps energy classes to scores with safe fallbacks

### Error Handling
- Missing GPS coordinates default to 0 points for train distance
- Missing energy class defaults to 3 points
- Missing lot size/basement size default to 0
- Safe type casting with fallbacks

### Performance Considerations
- UDFs are registered once per session
- Scoring calculations are vectorized where possible
- Final score rounded to 2 decimal places

## Changes from Previous Algorithm

### Removed Elements
- Dense ranking based on distinct value counts
- Room count scoring (rooms are now just a filter)
- Simple price scoring

### Added Elements
- Energy efficiency scoring (climate impact)
- Transportation accessibility (train proximity)
- Property size factors (lot size, basement)
- More sophisticated price efficiency calculation

### Weight Adjustments
- Prioritizes energy efficiency and transportation
- Reduces weight on market timing factors
- Balances property characteristics vs. financial metrics

## Database Schema

The enhanced algorithm outputs these columns:
- `total_score`: Weighted total (0-73.5)
- `score_energy`: Energy class points (0-10)
- `score_train_distance`: Train proximity points (0-10)
- `score_lot_size`: Lot size points (0-10)
- `score_house_size`: House size points (0-10)
- `score_price_efficiency`: Price efficiency points (0-10)
- `score_build_year`: Build year points (0-10)
- `score_basement`: Basement points (0-5)
- `score_days_market`: Market timing points (0-10)

## Testing and Validation

The algorithm has been validated to ensure:
- All scores fall within expected ranges
- UDFs handle edge cases gracefully
- Performance is acceptable for batch processing
- Results are reproducible and deterministic
