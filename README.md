ETL Pipeline: CSV, JSON, and XML

A Python-based ETL (Extract, Transform, Load) pipeline that consolidates data from multiple file formats (CSV, JSON, XML) into a single clean dataset. This project demonstrates strong data engineering and data wrangling skills, including schema alignment, data transformation, and logging for reproducibility.

🔑 Key Features

Multi-format extraction: Reads structured data from CSV, JSON, and XML files.

Transformation: Cleans and validates fields (name, height, weight) to ensure schema consistency.

Loading: Saves transformed data into a consolidated CSV file.

Logging: Tracks ETL phases with timestamps for transparency and debugging.

🛠️ Tech Stack

Python 3.11

Pandas for data manipulation

XML ElementTree for XML parsing

JSON library for flexible JSON parsing

📂 Project Structure
etl_code.py          # Main ETL pipeline script
source1.csv/json/xml # Example input data (multi-format)
transformed_data.csv # Final consolidated dataset
log_file.txt         # Log of ETL runs

🚀 Outcome

This pipeline demonstrates how to:
✔ Handle heterogeneous data sources
✔ Normalize them into a single schema
✔ Produce reproducible, auditable ETL workflows
