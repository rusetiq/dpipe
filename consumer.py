from kafka import KafkaConsumer
import psycopg2
from psycopg2 import OperationalError
import json
import os
import time
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka-consumer")

def get_db_connection(max_retries=5, backoff_base=2):
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                port=int(os.getenv("POSTGRES_PORT", 5432)),
                host=os.getenv("POSTGRES_HOST", "localhost"),
                database=os.getenv("POSTGRES_DB", "bank"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "admin")
            )
            logger.info("Connected to Postgres")
            return conn
        except OperationalError as e:
            if attempt < max_retries - 1:
                wait_time = backoff_base ** attempt
                logger.warning(f"DB connection failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to connect to Postgres after {max_retries} attempts")
                raise

def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id UUID PRIMARY KEY,
                amount NUMERIC(14,2),
                sender VARCHAR(64),
                receiver VARCHAR(64),
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        conn.commit()
    logger.info("Database schema initialized")

consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")],
    value_deserializer=lambda v: json.loads(v.decode()),
    group_id=os.getenv("CONSUMER_GROUP", "transactions-group"),
    auto_offset_reset="earliest",
    enable_auto_commit=False
)

conn = get_db_connection()
init_db(conn)

logger.info("Consumer listening for messages...")

for msg in consumer:
    try:
        data = msg.value
        
        required_keys = ["transaction_id", "amount", "sender", "receiver"]
        if not all(key in data for key in required_keys):
            logger.error(f"Invalid message structure: {data}")
            consumer.commit()
            continue
        
        logger.info(f"Received transaction: {data['transaction_id']}")
        
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO transactions (transaction_id, amount, sender, receiver)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    data["transaction_id"],
                    data["amount"],
                    data["sender"],
                    data["receiver"]
                ))
                conn.commit()
                logger.info(f"Transaction {data['transaction_id']} inserted successfully")
                consumer.commit()
        except OperationalError as e:
            logger.error(f"Database error processing {data.get('transaction_id')}: {e}")
            conn.close()
            conn = get_db_connection()
            init_db(conn)
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO transactions (transaction_id, amount, sender, receiver)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        data["transaction_id"],
                        data["amount"],
                        data["sender"],
                        data["receiver"]
                    ))
                    conn.commit()
                    logger.info(f"Transaction {data['transaction_id']} inserted on retry")
                    consumer.commit()
            except Exception as retry_error:
                logger.error(f"Retry failed for {data.get('transaction_id')}: {retry_error}")
        except Exception as e:
            logger.error(f"Unexpected error processing message: {e}")
            
    except Exception as e:
        logger.error(f"Error in message loop: {e}")
        time.sleep(1)
