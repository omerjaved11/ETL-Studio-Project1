# 🚀 ETL Data Pipeline — Python + PostgreSQL + Docker

This project implements a **modular ETL (Extract–Transform–Load) pipeline** using:

* **Python 3.11+**
* **PostgreSQL 15** (Dockerized)
* **Docker Compose**
* **YAML-based configuration**
* **Structured logging**
* **Configurable ingestion (CSV, JSON, API)**

The pipeline reads raw files, validates and cleans data, loads it into PostgreSQL, and logs every step.

---

## 📁 Project Structure

```
Revature-Cognizant-Project1/
│
├── src/
│   ├── main.py               # ETL entrypoint
│   ├── utils/
│   │   ├── db.py             # DB connection handling
│   │   ├── logger.py         # Project-wide logging
│   │   └── config.py         # YAML configuration loader
│
├── config/
│   └── config.yaml           # DB + ETL settings
│
├── data/                     # (mounted) input/output files
│
├── docker/
│   ├── etl.Dockerfile        # Dockerfile for ETL container
│   ├── postgresql.conf       # (optional) custom DB config
│   └── init.sql              # (optional) bootstrap SQL
│
├── docker-compose.yml
├── requirements.txt
├── .env                      # DO NOT COMMIT (contains secrets)
└── README.md
```

---

## 🔧 Technologies Used

| Component  | Technology              |
| ---------- | ----------------------- |
| Language   | Python 3.11             |
| DB         | PostgreSQL 15 (Docker)  |
| UI         | pgAdmin 4 (Docker)      |
| Config     | YAML                    |
| Logging    | Python logging module   |
| Deployment | Docker & Docker Compose |

---

## 🚨 Environment Setup

### 1. Install dependencies (local development)

```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

---

### 2. Create `.env` (NOT committed to Git)

```
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=postgres
POSTGRES_PORT=5434
```

> 💡 **We use port `5434` instead of `5432`** to avoid conflicts with any local PostgreSQL installation.

---

### 3. Start Postgres & pgAdmin via Docker

```bash
docker compose up -d postgres pgadmin
```

Check running containers:

```bash
docker compose ps
```

---

### 4. Connect with pgAdmin

* URL: [http://localhost:8080](http://localhost:8080)
* Email: `admin@example.com`
* Password: `admin`

Create new server:

| Field    | Value       |
| -------- | ----------- |
| Host     | `localhost` |
| Port     | `5434`      |
| Username | from `.env` |
| Password | from `.env` |
| Database | `postgres`  |

---

## ▶️ Running the ETL Pipeline (2 ways)

### **Option A — Run ETL locally (recommended during dev)**

```bash
python src/main.py
```

### **Option B — Run ETL inside Docker**

```bash
docker compose up --build etl
```

Or for logs:

```bash
docker compose logs -f etl
```

---

## ⚙️ Configuration (config/config.yaml)

Your ETL + DB settings live in:

```yaml
database:
  host: localhost
  port: 5434
  user: postgres
  password: postgres
  name: postgres

logging:
  log_dir: logs
  log_level: INFO
  log_format: "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
```

You may add:

* input file paths
* ingestion schedules
* API endpoint settings
* validation rules

---

## 🧪 Verifying DB Connection

Inside project root:

```bash
cd src
python -c "from utils.db import get_db_connection; c = get_db_connection(); print('connected:', c is not None); c and c.close()"
```

Expected:

```
connected: True
```

---

## 🐳 Docker Compose Services

### **etl**

* Runs Python ETL in container
* Mounted `/data` for real file access
* Uses `.env` for DB values

### **postgres**

* PostgreSQL 15 server
* Port mapped `5434:5432`
* Data persisted in Docker volume

### **pgadmin**

* Database GUI
* Accessible on `http://localhost:8080`

---

## 📦 Deployment

To deploy everything in containers:

```bash
docker compose up --build -d
```

To stop:

```bash
docker compose down
```

---

## 🔒 Security Notes

* **Never commit `.env`**
* Commit **`.env.example`** instead:

```
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
POSTGRES_PORT=
```

* Use environment variables in production (e.g., Docker Secrets)

---

## 📝 Git Workflow

Since you started on `main` but want to push code to a branch:

```bash
git checkout -b feature/etl-setup
git push -u origin feature/etl-setup
```

---

## 📌 Future Enhancements (optional)

* Add Alembic for DB migrations
* Add CSV/JSON ingestion pipelines
* Add API ingestion
* Add data validation layer (Pydantic)
* Add unit tests
* Add cron-based scheduling
* Add Airflow integration