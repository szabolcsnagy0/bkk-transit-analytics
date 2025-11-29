# BKK Transit Analytics

A complete ELT pipeline and analytics platform for Budapest public transit (BKK) data. This project collects real-time vehicle positions and weather data, transforms it into a Star Schema Data Warehouse, and visualizes insights using Metabase.

## 🏗️ Architecture

1.  **Data Collection**: Python scripts collect real-time data from BKK API and OpenWeather API.
2.  **Staging**: Raw JSON data is loaded into PostgreSQL staging tables.
3.  **Data Warehouse**: Data is transformed into a Star Schema (Fact/Dimensions) in PostgreSQL.
4.  **Analytics Mart**: SQL Views enrich the data with business logic (e.g., "Morning", "Rainy").
5.  **Visualization**: Metabase dashboard for interactive reporting.

> Created in 2025 for the Business Intelligence course.
