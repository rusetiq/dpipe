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
- **Kafka** (port 9092) - Message broker
- **PostgreSQL** (port 5432) - Database
- **Flask** (port 5000) - Web application
- **Consumer** - Reads from Kafka and stores in PostgreSQL

### 4. Access the Application

- **Submit Transactions**: http://localhost:5000
- **Admin Dashboard**: http://localhost:5000/admin

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
  - ✓ **Delivered** - Successfully delivered to Kafka and stored in DB
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
| `/admin` | GET | Admin dashboard |
| `/api/admin-data` | GET | Returns queue times, transaction log, and DB tables in JSON |
| `/verify-db` | GET | Verify database connection and view transaction count |
| `/clear-data` | POST | Delete queue logs and transaction logs |
| `/drop-database` | POST | Reset database - drops and recreates transactions table |
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

### Database Connection Issues

**Error**: `Cannot connect to database`

**Solution**:
- Verify PostgreSQL is running: `docker-compose ps postgres`
- Check credentials in `.env` file
- Verify PostgreSQL container health: `docker-compose logs postgres`

### Flask App Not Accessible

**Error**: `Connection refused on localhost:5000`

**Solution**:
- Check if Flask is running: `docker-compose ps flask`
- View Flask logs: `docker-compose logs flask`
- Ensure port 5000 is not in use by other services

### Transactions Stuck in "Pending"

**Issue**: Transactions show "Pending" but should be delivered

**Solution**:
- Check consumer logs: `docker-compose logs consumer`
- Verify database table exists and is accessible
- Check queue_log.json for delivery timestamps
- Try clearing data and resubmitting

## Development

### Local Setup (Without Docker)

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

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
├── README.md              # This file
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

### Kafka Consumer (`consumer.py`)

- Listens to "transactions" topic
- Validates message structure
- Inserts transactions into PostgreSQL
- Implements exponential backoff for DB failures
- Auto-reconnects on network issues

### Admin Dashboard (`templates/admin.html`)

- Real-time data visualization
- 3-second refresh interval via JavaScript fetch
- Shows transaction status lifecycle
- Liquid glass UI design with dithered background effect

## Configuration

### Kafka Settings

- **Broker**: kafka:9092 (inside Docker network)
- **Topic**: transactions
- **Consumer Group**: transactions-group
- **Auto Offset Reset**: earliest

### PostgreSQL Settings

- **Host**: postgres (inside Docker network)
- **Port**: 5432
- **Database**: bank
- **User**: pipelinetest
- **Password**: pipeline

### Flask Settings

- **Debug Mode**: Enabled
- **Host**: 0.0.0.0 (accessible from outside container)
- **Port**: 5000
- **Rate Limit**: 100 requests/minute
- **Session Security**: HTTPONLY, SAMESITE=Strict

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
```

## Deployment

### Production Considerations

1. **Change Default Credentials**
   - Update admin login in `app.py`
   - Generate strong SECRET_KEY

2. **Security**
   - Set `SESSION_COOKIE_SECURE = True` (requires HTTPS)
   - Use environment variables for all secrets
   - Implement HTTPS/TLS

3. **Database**
   - Use managed PostgreSQL service
   - Implement backups
   - Monitor performance

4. **Kafka**
   - Use managed Kafka service or cluster setup
   - Configure replication factor > 1
   - Monitor broker health

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

## Support

For issues or questions:
1. Check troubleshooting section above
2. Review logs: `docker-compose logs`
3. Verify `.env` configuration
4. Check PostgreSQL and Kafka connectivity

---

**Last Updated**: December 2025  
**Version**: 1.0.0
