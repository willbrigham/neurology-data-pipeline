# NeuroFinder – Real-Time Doctor & Hospital Search Platform

**NeuroFinder** is a real-time data pipeline designed to help patients find nearby neurologists and hospitals quickly and reliably, especially after medical events like strokes.

## Features
- Stream ingestion of doctor/hospital data via Kafka
- Batch and real-time processing using Apache Spark and Flink
- Central data warehouse in Snowflake
- Optional REST API for doctor search

## Architecture
![architecture](docs/architecture.png)

## Tech Stack
- Apache Kafka
- Apache Spark (batch)
- Apache Flink (stream)
- Snowflake
- Python, FastAPI
- Airflow (pipeline orchestration)

## Project Structure
- `kafka/`: Data producers for Kafka streams
- `spark_jobs/`: Batch ETL jobs to clean and enrich raw doctor data
- `flink_jobs/`: Streaming pipeline for real-time updates
- `snowflake/`: Data warehouse schema and queries
- `api/`: Optional REST API for search

## Getting Started
Instructions to run each part locally or via Docker

## Inspiration
This project was inspired by my own difficulty in finding a neurologist upon release from the ICU. From my experience, I found that once an individual is released from the hospital, it is nearly impossible to get timely appointments. In my case, I wanted to run extensive tests after having a stroke as a healthy 24 YOM. However, I was forced to wait nearly 2 months before I was able to see a neurologist due to paperwork, insurance, and the neurology office not having any available appointments. Waiting this long to find answers about a life threatening event that has a high chance of reacurring is ludicrous. I am sure that I am not the only one that has been frustrated, scared, and felt helpless by this system. My goal is to make the search and access to medical specialists much easier than what I experinced, powered by modern data tools.
