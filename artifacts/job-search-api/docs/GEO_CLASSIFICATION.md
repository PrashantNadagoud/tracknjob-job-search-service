# Geo-Restriction Classification Reference

## The Problem
Many "Remote" jobs are restricted to specific regions due to tax, legal, or time zone requirements. Users in India don't want to see "Remote US" jobs.

## Classification Values
- **`US`**: Restricted to United States (On-site, Hybrid, or US-Remote).
- **`EU`**: Restricted to European Union / UK.
- **`IN`**: Restricted to India.
- **`GLOBAL`**: Truly remote with no geographic hiring restriction.
- **`null`**: Legacy rows (treated as US in the search API).

## Classification Logic (`classify_listing`)

### 1. Structured Signals (Priority 1)
If the ATS provides a structured ISO country code:
- `US`, `USA` -> `US`
- `GB`, `DE`, `FR`, etc. (EU list) -> `EU`
- `IN`, `INDIA` -> `IN`

### 2. Heuristic Heuristics (Priority 2)
If no structured country is found, `detect_geo_restriction()` scans the location string and the first 2000 characters of the description for keywords:

- **US Signals**: "United States", "USA", "New York", "Remote US", "US Only".
- **EU Signals**: "Germany", "France", "UK", "Netherlands", "EMEA", "CET", "Right to work in Europe".
- **India Signals**: "India", "Bangalore", "Hyderabad", "Pune", "Mumbai".

**Note**: US signals take priority if multiple regions are mentioned.

### 3. Work Type Fallback (Priority 3)
If no geographic signals are detected:
- If `work_type` is `remote` or `fully_remote`, the listing is classified as `GLOBAL`.
- Otherwise, it defaults to `US`.

## ATS-Specific Parsing

### Greenhouse
Uses `parse_greenhouse_location()` which prioritizes the structured `offices[]` array. It scans office names and locations for the same signals.

### Ashby
Uses `parse_ashby_location()` which checks `officeLocations[].countryCode` first.

## API Integration
The `?market=` query parameter in the search API maps directly to these filters:
- `market=EU` -> `geo_restriction IN ('EU', 'GLOBAL')`
- `market=IN` -> `geo_restriction IN ('IN', 'GLOBAL')`
- `market=US` (Default) -> `geo_restriction IN ('US', 'GLOBAL')` OR `is null`
