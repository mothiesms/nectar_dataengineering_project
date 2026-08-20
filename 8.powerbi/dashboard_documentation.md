# Power BI Dashboard Documentation

## Title
**NECTAR ENERGY & ASSET PERFORMANCE DASHBOARD**

**Site Performance | Asset Health | Energy Consumption | Fault Analysis**

## Requirement mapping
- Site metrics → Total Sites, Energy by Site, Site Performance Summary
- Asset metrics → Total Assets, Assets with Faults, Energy by Asset, Fault ranking
- Energy trends → Daily Energy Consumption Trend
- Fault statistics → Faults by Event Type, Fault Distribution by Severity

## Recommended KPI cards
1. Total Sites
2. Total Assets
3. Assets with Faults
4. Total Readings
5. Total Faults

## Recommended visuals
- Energy Consumption by Site
- Energy Consumption by Asset
- Daily Energy Consumption Trend
- Top Assets by Fault Count
- Faults by Event Type
- Fault Distribution by Severity
- Site Performance Summary
- Date slicer

## Relationships
Use one-to-many and single-direction Dimension → Fact filtering. Avoid fact-to-fact relationships and avoid multiple active paths between a fact and the same dimension.
