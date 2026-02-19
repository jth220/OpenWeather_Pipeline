# OpenWeather Ingestion Pipeline

Minimal cloud-oriented ingestion script that retrieves current weather data from the OpenWeather API, performs validation and safety checks, and stores raw events in a Bronze-style object layout.

This repository serves as a reference implementation for ingestion reliability, idempotent storage, and structured observability within a simple data pipeline.

## Functionality

- Fetches current weather data for a configured city
- Retries failed requests using exponential backoff (e.g., rate limits, server errors, network failures)
- Applies connection and read timeouts
- Quarantines malformed, oversized, or client-error payloads
- Produces structured logs with secret redaction
- Generates deterministic event identifiers (`city_id + dt → SHA1`)
- Partitions Bronze data by city, date, and hour

## Repository Structure

.
├── ingest_openweather.py  
├── settings.env.example  
├── .gitignore  
├── requirements.txt  
└── README.md  

## Design Principles

- Explicit success and failure handling
- Idempotent event storage
- Observable execution through structured logging
- Environment-driven configuration and secret management

## Planned Extensions

- Bronze object writer implementation
- Manifest metadata alongside stored events
- Workflow orchestration integration
- Infrastructure provisioning scripts