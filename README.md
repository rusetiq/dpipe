# Data Pipeline - Transaction Processing System

A real-time transaction data pipeline built with Kafka, PostgreSQL, and Flask. Submit transactions through a web form, process them via Kafka, store in PostgreSQL, and monitor everything through an admin dashboard.

## Architecture

```
User Input (Web Form)
        ↓
    Kafka Topic
        ↓
PostgreSQL Database
        ↓
Admin Dashboard (Real-time Monitoring)
```

## Prerequisites

- **Docker & Docker Compose** - Containerization platform
- **Python 3.9+** - Runtime (if running locally)
- **Git** - For cloning the repository
- **Kafka 7.5.0+** - Message broker (included in Docker, optional for local setup)
- **PostgreSQL 15+** - Database (included in Docker)

## Installation Guide

### Step 1: Install Required Tools

#### Docker & Docker Compose

**Windows:**
1. Download Docker Desktop from https://www.docker.com/products/docker-desktop
2. Run the installer and follow the setup wizard
3. Restart your computer when prompted
4. Verify installation in PowerShell:
   ```powershell
   docker --version
   docker-compose --version
   ```

**macOS:**
1. Download Docker Desktop from https://www.docker.com/products/docker-desktop
2. Open the DMG file and drag Docker to Applications
3. Open Docker from Applications
4. Verify installation in Terminal:
   ```bash
   docker --version
   docker-compose --version
   ```

**Linux (Ubuntu/Debian):**
```bash
sudo apt-get update
sudo apt-get install docker.io docker-compose
sudo usermod -aG docker $USER
# Log out and back in for group changes to take effect
docker --version
docker-compose --version
```

#### Python 3.9+

**Windows:**
1. Download from https://www.python.org/downloads/
2. Run the installer
3. **IMPORTANT**: Check "Add Python to PATH"
4. Click "Install Now"
5. Verify in PowerShell:
   ```powershell
   python --version
   pip --version
   ```

**macOS:**
```bash
# Using Homebrew (install from https://brew.sh if needed)
brew install python@3.11
python3 --version
pip3 --version
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt-get install python3 python3-pip
python3 --version
pip3 --version
```

#### Git

**Windows:**
1. Download from https://git-scm.com/download/win
2. Run the installer with default settings
3. Verify in PowerShell:
   ```powershell
   git --version
   ```

**macOS:**
```bash
brew install git
git --version
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt-get install git
git --version
```

### Step 2: Clone the Repository

```bash
git clone https://github.com/rusetiq/dpipe.git
cd datapipeline
```

### Step 3: Environment Setup

Create a `.env` file in the project root with these settings:

```env
SECRET_KEY=your_dev_secret_key_here_change_in_production
POSTGRES_HOST=postgres
POSTGRES_DB=bank
POSTGRES_USER=pipelinetest
POSTGRES_PASSWORD=pipeline
POSTGRES_PORT=5432
KAFKA_BOOTSTRAP=kafka:9092
KAFKA_BROKER=kafka:9092
```

### Step 4: Docker Compose Setup

This project uses Docker Compose to orchestrate all services. No separate installation of Kafka or PostgreSQL is needed!

**What gets installed automatically:**
- **Apache Kafka 7.5.0** with ZooKeeper
- **PostgreSQL 15** database
- **Python Flask** web application
- **Kafka Consumer** service

#### Start Services

```bash
docker-compose up -d
```

This command:
- Pulls container images from Docker Hub
- Creates containers for all services
- Starts services in the background
- Mounts volumes for data persistence

#### Verify Services are Running

```bash
docker-compose ps
```

You should see:
```
NAME            STATUS
zookeeper       Up
kafka           Up
postgres        Up
flask           Up
consumer        Up
```

#### View Logs

```bash
# View all service logs
docker-compose logs

# View specific service
docker-compose logs flask
docker-compose logs consumer
docker-compose logs kafka
docker-compose logs postgres

# Follow logs in real-time
docker-compose logs -f flask
```

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/rusetiq/dpipe.git
cd datapipeline
```

### 2. Environment Setup

Create a `.env` file in the project root:

```env
SECRET_KEY=your_dev_secret_key_here
POSTGRES_HOST=postgres
POSTGRES_DB=bank
POSTGRES_USER=pipelinetest
POSTGRES_PASSWORD=pipeline
POSTGRES_PORT=5432
KAFKA_BROKER=kafka:9092
```

### 3. Start Services with Docker Compose

```bash
docker-compose up -d
```

This starts:
- **ZooKeeper** (port 2181) - Kafka coordination
- **Kafka** (port 9092) - Message broker (Apache Kafka 7.5.0)
- **PostgreSQL** (port 5432) - Database (PostgreSQL 15)
- **Flask** (port 5000) - Web application
- **Consumer** - Reads from Kafka and stores in PostgreSQL

### 4. Access the Application

- **Submit Transactions**: http://localhost:5000
- **Admin Dashboard**: http://localhost:5000/admin

## Kafka Details

### About Kafka

Apache Kafka is a distributed event streaming platform. In this project:
- **Version**: 7.5.0 (Confluent platform)
- **Topic**: `transactions` - Receives transaction messages
- **Consumer Group**: `transactions-group` - Processes messages
- **Partitions**: 1 (default)
- **Replication Factor**: 1 (default for development)

### Kafka Installation (Docker)

The `docker-compose.yml` includes:

```yaml
kafka:
  image: confluentinc/cp-kafka:7.5.0
  ports:
    - "9092:9092"
  environment:
    KAFKA_BROKER_ID: 1
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
  depends_on:
    - zookeeper
```

### Kafka Without Docker (Advanced)

If you want to run Kafka locally without Docker:

1. Download from https://kafka.apache.org/downloads
2. Extract to a location (e.g., `C:\kafka` on Windows)
3. Start ZooKeeper:
   ```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
   ```
4. Start Kafka broker in another terminal:
   ```bash
   bin/kafka-server-start.sh config/server.properties
   ```
5. Create topic:
   ```bash
   bin/kafka-topics.sh --create --topic transactions \
     --bootstrap-server localhost:9092 \
     --partitions 1 --replication-factor 1
   ```

## PostgreSQL Details

### About PostgreSQL

PostgreSQL is an open-source relational database. In this project:
- **Version**: 15
- **Database**: `bank`
- **Default User**: `pipelinetest`
- **Default Password**: `pipeline`
- **Port**: 5432

### PostgreSQL Installation (Docker)

The `docker-compose.yml` includes:

```yaml
postgres:
  image: postgres:15
  ports:
    - "5432:5432"
  environment:
    POSTGRES_DB: bank
    POSTGRES_USER: pipelinetest
    POSTGRES_PASSWORD: pipeline
```

### Connecting to PostgreSQL

**From Docker:**
```bash
docker-compose exec postgres psql -U pipelinetest -d bank
```

**From Local Machine (if Docker installed):**
```bash
# Install psql client
# Windows: https://www.postgresql.org/download/windows/
# macOS: brew install postgresql
# Linux: sudo apt-get install postgresql-client

psql -h localhost -U pipelinetest -d bank
# Enter password: pipeline
```

**Common PostgreSQL Commands:**
```sql
-- View all tables
\dt

-- View transactions table
SELECT * FROM transactions;

-- Count transactions
SELECT COUNT(*) FROM transactions;

-- View recent transactions
SELECT * FROM transactions ORDER BY created_at DESC LIMIT 10;

-- Exit
\q
```

### PostgreSQL Without Docker (Advanced)

1. **Windows**: Download from https://www.postgresql.org/download/windows/
2. **macOS**: `brew install postgresql`
3. **Linux**: `sudo apt-get install postgresql postgresql-contrib`

Then create the database:
```bash
psql -U postgres
CREATE DATABASE bank;
CREATE USER pipelinetest WITH PASSWORD 'pipeline';
GRANT ALL PRIVILEGES ON DATABASE bank TO pipelinetest;
```

## Usage

### Submit a Transaction

1. Visit http://localhost:5000
2. Fill in the form:
   - **Sender**: Username (5-64 alphanumeric characters, hyphens, underscores)
   - **Receiver**: Username (same format)
   - **Amount**: Transaction amount (decimal)
3. Click "Submit Transaction"
4. View status immediately or check admin dashboard

### Admin Dashboard

1. Visit http://localhost:5000/admin
2. Login with credentials:
   - **Username**: `aarush`
   - **Password**: `Aarush@101109`
3. View real-time data:
   - **Queue Log** - Shows pending and delivered transactions
   - **Transaction Log** - All submitted transactions with timestamps
   - **Database Tables** - View actual data stored in PostgreSQL

### Dashboard Features

- **Auto-Refresh**: Updates every 3 seconds automatically
- **Status Tracking**: 
  - ⏳ **Pending** - Queued but not yet delivered to Kafka
  - ⏳ **Verifying...** - Delivered to Kafka, checking database
  - ✓ **Complete** - Successfully stored in PostgreSQL
- **Clear Data**: Remove all pending queue logs and transaction logs (button in header)
- **Drop Database**: Reset and recreate the transactions table

## API Endpoints

### Public Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET, POST | Submit transactions |
| `/login` | GET, POST | Admin authentication |

### Protected Endpoints (Require Authentication)

| Endpoint | Method | Description |
|----------|--------|---------------|
| `/admin` | GET | Admin dashboard |
| `/api/admin-data` | GET | Returns queue times, transaction log, and DB tables in JSON |
| `/verify-db` | GET | Verify database connection and view transaction count |
| `/clear-data` | POST | Delete queue logs and transaction logs |
| `/drop-database` | POST | Reset database by dropping and recreating transactions table |
| `/logout` | GET | Logout and clear session |

## Data Files

The application generates local JSON files for logging:

- **`queue_log.json`** - Tracks when transactions are queued and delivered
  ```json
  [
    {
      "transaction_id": "uuid",
      "event_type": "queued|delivered",
      "timestamp": "ISO 8601 timestamp"
    }
  ]
  ```

- **`transactions.json`** - Stores transaction details with local timestamps
  ```json
  [
    {
      "transaction_id": "uuid",
      "sender": "user1",
      "receiver": "user2",
      "amount": 100.00,
      "queued_at": "ISO 8601 timestamp",
      "database_logged_at": "ISO 8601 timestamp"
    }
  ]
  ```

## Database Schema

### PostgreSQL `transactions` Table

```sql
CREATE TABLE transactions (
    transaction_id UUID PRIMARY KEY,
    amount DECIMAL(10, 2),
    sender VARCHAR(255),
    receiver VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Troubleshooting

### Kafka Connection Issues

**Error**: `NoBrokersAvailable`

**Solution**: 
- Ensure Kafka container is running: `docker-compose ps`
- Check Kafka logs: `docker-compose logs kafka`
- Restart services: `docker-compose restart`
- Verify Kafka is listening: `docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092`

### Database Connection Issues

**Error**: `Cannot connect to database`

**Solution**:
- Verify PostgreSQL is running: `docker-compose ps postgres`
- Check credentials in `.env` file
- Verify PostgreSQL container health: `docker-compose logs postgres`
- Test connection: `docker-compose exec postgres psql -U pipelinetest -d bank -c "SELECT 1;"`

### Flask App Not Accessible

**Error**: `Connection refused on localhost:5000`

**Solution**:
- Check if Flask is running: `docker-compose ps flask`
- View Flask logs: `docker-compose logs flask`
- Ensure port 5000 is not in use by other services
- Try accessing from browser: http://localhost:5000

### Transactions Stuck in "Pending"

**Issue**: Transactions show "Pending" but should be delivered

**Solution**:
- Check consumer logs: `docker-compose logs consumer`
- Verify database table exists and is accessible
- Check queue_log.json for delivery timestamps
- View database transactions: `docker-compose exec postgres psql -U pipelinetest -d bank -c "SELECT * FROM transactions;"`
- Try clearing data and resubmitting

### Port Already in Use

**Error**: `bind: address already in use`

**Solution**:
```bash
# Find process using port
# Windows PowerShell
Get-NetTCPConnection -LocalPort 5000

# Kill the process
Stop-Process -Id <PID> -Force

# Or change port in docker-compose.yml
# Change "5000:5000" to "5001:5000" for Flask
```

### Docker Issues

**Error**: `Cannot connect to Docker daemon`

**Solution**:
- Start Docker Desktop (Windows/macOS)
- On Linux, start Docker service: `sudo systemctl start docker`
- Verify Docker is running: `docker ps`

## Development

### Local Setup (Without Docker)

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create database
psql -U postgres -c "CREATE DATABASE bank;"
psql -U postgres -c "CREATE USER pipelinetest WITH PASSWORD 'pipeline';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE bank TO pipelinetest;"

# Create transactions table
psql -U pipelinetest -d bank -f schema.sql

# Run Flask app
python app.py

# Run consumer in separate terminal
python consumer.py
```

### Project Structure

```
datapipeline/
├── app.py                 # Flask application
├── consumer.py            # Kafka consumer
├── docker-compose.yml     # Service orchestration
├── Dockerfile             # Container configuration
├── requirements.txt       # Python dependencies
├── .gitignore             # Git ignore file
├── README.md              # This file
├── .env                   # Environment variables (create this)
├── templates/
│   ├── index.html         # Transaction submission form
│   ├── login.html         # Admin login page
│   └── admin.html         # Admin dashboard
├── queue_log.json         # Generated - transaction queue events
└── transactions.json      # Generated - transaction records
```

## Key Components

### Flask App (`app.py`)

- Handles HTTP requests and responses
- Manages user authentication and sessions
- Produces messages to Kafka topic
- Serves static dashboard with real-time updates
- Rate-limited to 100 requests/minute per IP
- Database connection pooling with psycopg2

### Kafka Consumer (`consumer.py`)

- Listens to "transactions" topic
- Validates message structure
- Inserts transactions into PostgreSQL
- Implements exponential backoff for DB failures
- Auto-reconnects on network issues
- Consumer group: `transactions-group`

### Admin Dashboard (`templates/admin.html`)

- Real-time data visualization
- 3-second refresh interval via JavaScript fetch
- Shows transaction status lifecycle
- Liquid glass UI design with dithered background effect
- Client-side database verification for transactions

## Configuration

### Kafka Settings

- **Broker**: kafka:9092 (inside Docker network)
- **Topic**: transactions
- **Consumer Group**: transactions-group
- **Auto Offset Reset**: earliest
- **Partitions**: 1
- **Replication Factor**: 1

### PostgreSQL Settings

- **Host**: postgres (inside Docker network)
- **Port**: 5432
- **Database**: bank
- **User**: pipelinetest
- **Password**: pipeline
- **Connection Pool**: psycopg2 with automatic reconnection

### Flask Settings

- **Debug Mode**: Enabled
- **Host**: 0.0.0.0 (accessible from outside container)
- **Port**: 5000
- **Rate Limit**: 100 requests/minute
- **Session Security**: HTTPONLY, SAMESITE=Strict
- **Form Validation**: WTForms with CSRF protection

## Monitoring

### View Logs

```bash
# All services
docker-compose logs

# Specific service
docker-compose logs flask
docker-compose logs consumer
docker-compose logs kafka
docker-compose logs postgres

# Follow logs
docker-compose logs -f flask
```

### Check Service Status

```bash
docker-compose ps
```

### Access PostgreSQL Directly

```bash
docker-compose exec postgres psql -U pipelinetest -d bank
```

Inside psql:
```sql
SELECT * FROM transactions;
SELECT COUNT(*) FROM transactions;
SELECT * FROM transactions ORDER BY created_at DESC LIMIT 10;
```

### Check Kafka Topics

```bash
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Monitor Kafka Consumer

```bash
docker-compose exec kafka kafka-consumer-groups \
  --list \
  --bootstrap-server localhost:9092
```

## Deployment

### Production Considerations

1. **Change Default Credentials**
   - Update admin login in `app.py`
   - Generate strong SECRET_KEY
   - Use environment variables for all secrets

2. **Security**
   - Set `SESSION_COOKIE_SECURE = True` (requires HTTPS)
   - Use environment variables for all secrets
   - Implement HTTPS/TLS
   - Run behind a reverse proxy (nginx, Apache)

3. **Database**
   - Use managed PostgreSQL service (AWS RDS, Azure Database)
   - Implement automated backups
   - Monitor performance and resource usage
   - Set up connection pooling (PgBouncer)

4. **Kafka**
   - Use managed Kafka service (AWS MSK, Confluent Cloud)
   - Configure replication factor > 1
   - Monitor broker health
   - Set up proper retention policies

5. **Monitoring & Logging**
   - Set up centralized logging (ELK stack, Datadog)
   - Monitor application metrics
   - Set up alerts for errors
   - Use APM (Application Performance Monitoring)

## Common Commands

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# View specific service logs
docker-compose logs -f <service-name>

# Restart a service
docker-compose restart <service-name>

# Execute command in container
docker-compose exec <service> <command>

# Remove volumes (deletes database data)
docker-compose down -v

# Rebuild images
docker-compose build --no-cache

# Check resource usage
docker stats
```

## API Examples

### Submit Transaction

```bash
curl -X POST http://localhost:5000/ \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "sender=alice&receiver=bob&amount=50.00"
```

### Login

```bash
curl -X POST http://localhost:5000/login \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=aarush&password=Aarush@101109" \
  -c cookies.txt
```

### Get Admin Data

```bash
curl http://localhost:5000/api/admin-data \
  -b cookies.txt
```

### Verify Database

```bash
curl http://localhost:5000/verify-db \
  -b cookies.txt
```

### Drop Database

```bash
curl -X POST http://localhost:5000/drop-database \
  -b cookies.txt
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

MIT License - See LICENSE file for details

## Support

For issues or questions:
1. Check troubleshooting section above
2. Review logs: `docker-compose logs`
3. Verify `.env` configuration
4. Check PostgreSQL and Kafka connectivity
5. Open an issue on GitHub

## Resources

- **Apache Kafka**: https://kafka.apache.org/
- **PostgreSQL**: https://www.postgresql.org/
- **Docker**: https://www.docker.com/
- **Flask**: https://flask.palletsprojects.com/
- **Confluent Platform**: https://www.confluent.io/

---

**Last Updated**: December 2025  
**Developer**: Aarush Diwakar (@rusetiq)

## Usage

### Submit a Transaction

1. Visit http://localhost:5000
2. Fill in the form:
   - **Sender**: Username (5-64 alphanumeric characters, hyphens, underscores)
   - **Receiver**: Username (same format)
   - **Amount**: Transaction amount (decimal)
3. Click "Submit Transaction"
4. View status immediately or check admin dashboard

### Admin Dashboard

1. Visit http://localhost:5000/admin
2. Login with credentials:
   - **Username**: `aarush`
   - **Password**: `Aarush@101109`
3. View real-time data:
   - **Queue Log** - Shows pending and delivered transactions
   - **Transaction Log** - All submitted transactions with timestamps
   - **Database Tables** - View actual data stored in PostgreSQL

### Dashboard Features

- **Auto-Refresh**: Updates every 3 seconds automatically
- **Status Tracking**: 
  - ⏳ **Pending** - Queued but not yet delivered to Kafka
  - ⏳ **Verifying...** - Delivered to Kafka, checking database
  - ✓ **Complete** - Successfully stored in PostgreSQL
- **Clear Data**: Remove all pending queue logs and transaction logs (button in header)
- **Drop Database**: Reset and recreate the transactions table
