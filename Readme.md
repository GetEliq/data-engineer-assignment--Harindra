# Energy Consumption Data Platform

The system ingests parquet data, engineers features, stores results in SQLite, and runs SQL-based analytics for insights.
 
## 📂 Repository Structure
src/
├── etl/
│ └── etl_run.py # Main ETL pipeline (read → transform → save)
├── features/
│ └── feature_generator.py # Feature engineering (time, holiday, rolling, seasonal, etc.)
├── utils/
│ ├── db_utils.py # DB connection + writes to SQLite
│ └── logger.py # Unified logging (console + CSV)
├── data_io/
│ └── reader.py # Read parquet with correct schema
├── config/
│ └── settings.py # Config paths (data/, db, logs, outputs)
└── sql/
├── queries.py # Predefined SQLite queries
└── run_queries.py # Run queries → save results in sql/outputs/
main.py # Entrypoint → calls run_etl()
requirements.txt # Python dependencies
instructions.txt # how to run the code


Features Generated

- **Time features** (hour, day-of-week, weekend, month, year): capture temporal usage patterns.  
- **Lag features** (1h, 24h): measure short-term consumption memory.  
- **Rolling stats** (24h mean/std): smooth fluctuations and identify abnormal spikes.  
- **Daily aggregates** (total, mean, peak hour, peak kWh): summarize load profiles.  
- **Rolling 7d/30d averages**: detect weekly/monthly trends.  
- **Consumption categories** (very low → high): classify usage intensity.  
- **Seasonality & holidays**: flag winter/summer effects, public holidays, and vacation months.  
- **Monthly aggregates**: monitor long-term consumption patterns.  

These features provide a holistic view of consumer behavior, allowing for:  
- Peak demand detection  
- Seasonal trend analysis  
- Impact of holidays/vacations  
- Customer segmentation by consumption profile

