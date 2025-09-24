# 🌦️ Hello Cloud – OpenWeather Ingestion Pipeline

This repo is a minimal **cloud-native ingestion system** that fetches data from the [OpenWeather API](https://openweathermap.org/current), applies safety checks, and lands raw events in a **Bronze object store layout**.

It’s built as a teaching project in cloud data engineering, covering contracts, retries, quarantine, and idempotent storage.

---

## 🚀 Features

- Fetches current weather for a configured city
- **Retries with exponential backoff** (handles 429, 5xx, network errors)
- **Timeouts** for connect + read
- **Quarantine** of malformed/oversized payloads or client errors
- **Structured logging** with secret redaction
- Deterministic **event IDs** (`city_id + dt → SHA1 hex`)
- Bronze partitioning by `city` / `date` / `hour`

---

## 📂 Repo structure

```
.
├── ingest_openweather.py   # main fetcher
├── settings.env.example    # sample config (copy → settings.env with real values)
├── .gitignore              # ignore venv, secrets, caches
├── requirements.txt        # Python dependencies
└── README.md               # this file
```

---

## 🛡️ Safety principles

- **Contracts**: explicit success/failure conditions
- **Idempotency**: same input → same event_id + path
- **Observability**: logs make pipeline a glass box, not a black box
- **Cloud hygiene**: redact secrets, send polite headers, env-driven config

---

## 🔮 Roadmap

- [ ] Implement Bronze object writer (`write_bronze_line`)
- [ ] Add manifest JSON alongside each Bronze write
- [ ] Wire into Prefect for orchestration
- [ ] Terraform scripts for infra (buckets, queues)

---

## 🤝 Contributing

This project is for learning purposes, but PRs and issues are welcome.  

---


