from flask import Flask, render_template, request, jsonify, session, redirect, url_for, flash
from flask_wtf import FlaskForm
from wtforms import DecimalField, StringField, PasswordField
from wtforms.validators import DataRequired, NumberRange, Length
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from kafka import KafkaProducer
from dotenv import load_dotenv
import os, json, uuid, re, logging, psycopg2
from datetime import datetime
from pathlib import Path
from functools import wraps

load_dotenv()

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev_secret")
app.config["SESSION_COOKIE_SECURE"] = False
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Strict"

TRANSACTION_LOG = "transactions.json"
QUEUE_LOG = "queue_log.json"

limiter = Limiter(get_remote_address, app=app, default_limits=["100/minute"])

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("flask-app")

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("logged_in"):
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated_function

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            database=os.getenv("POSTGRES_DB", "bank"),
            user=os.getenv("POSTGRES_USER", "pipelinetest"),
            password=os.getenv("POSTGRES_PASSWORD", "pipeline"),
            port=os.getenv("POSTGRES_PORT", "5432")
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

def delivery_report(err, msg, transaction_id):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        log_queue_event(transaction_id, "delivered", datetime.now().isoformat())

def log_transaction(transaction_data):
    try:
        transactions = []
        if Path(TRANSACTION_LOG).exists():
            with open(TRANSACTION_LOG, 'r') as f:
                transactions = json.load(f)
        
        transactions.append({
            **transaction_data,
            "queued_at": datetime.now().isoformat(),
            "database_logged_at": None
        })
        
        with open(TRANSACTION_LOG, 'w') as f:
            json.dump(transactions, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to log transaction: {e}")

def log_queue_event(transaction_id, event_type, timestamp):
    try:
        queue_log = []
        if Path(QUEUE_LOG).exists():
            with open(QUEUE_LOG, 'r') as f:
                queue_log = json.load(f)
        
        queue_log.append({
            "transaction_id": transaction_id,
            "event_type": event_type,
            "timestamp": timestamp
        })
        
        with open(QUEUE_LOG, 'w') as f:
            json.dump(queue_log, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to log queue event: {e}")

def get_database_transactions():
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM transactions ORDER BY created_at DESC LIMIT 100;")
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch from database: {e}")
        return []

def get_database_tables():
    conn = get_db_connection()
    if not conn:
        return {}
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public';
        """)
        tables = [row[0] for row in cur.fetchall()]
        
        table_data = {}
        for table in tables:
            cur.execute(f"SELECT * FROM {table} ORDER BY 1 DESC LIMIT 50;")
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            table_data[table] = {
                "columns": columns,
                "rows": [dict(zip(columns, row)) for row in rows]
            }
        
        cur.close()
        conn.close()
        return table_data
    except Exception as e:
        logger.error(f"Failed to fetch database tables: {e}")
        return {}

def transaction_exists_in_db(transaction_id):
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM transactions WHERE transaction_id = %s;", (transaction_id,))
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result is not None
    except Exception as e:
        logger.error(f"Failed to check transaction in database: {e}")
        return False

producer = KafkaProducer(
    bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")],
    value_serializer=lambda v: json.dumps(v).encode()
)

class TransactionForm(FlaskForm):
    amount = DecimalField("Amount", validators=[DataRequired(), NumberRange(min=0.01, max=1000000)])
    sender = StringField("Sender", validators=[DataRequired(), Length(min=5, max=64)])
    receiver = StringField("Receiver", validators=[DataRequired(), Length(min=5, max=64)])

class LoginForm(FlaskForm):
    username = StringField("Username", validators=[DataRequired()])
    password = PasswordField("Password", validators=[DataRequired()])

@app.route("/", methods=["GET", "POST"])
@limiter.limit("5/minute")
def index():
    form = TransactionForm()
    success = None
    error = None

    if form.validate_on_submit():
        amount = float(form.amount.data)
        sender = form.sender.data.strip()
        receiver = form.receiver.data.strip()

        if not re.match(r"^[A-Za-z0-9_-]{5,64}$", sender):
            error = "Invalid sender"
        elif not re.match(r"^[A-Za-z0-9_-]{5,64}$", receiver):
            error = "Invalid receiver"
        else:
            msg = {
                "transaction_id": str(uuid.uuid4()),
                "amount": amount,
                "sender": sender,
                "receiver": receiver
            }

            try:
                def callback(metadata):
                    delivery_report(None, metadata, msg["transaction_id"])
                def errback(exc):
                    delivery_report(exc, None, msg["transaction_id"])
                
                producer.send("transactions", msg).add_callback(callback).add_errback(errback)
                producer.flush(timeout=5)
                logger.info(f"Transaction queued: {msg['transaction_id']}")
                log_transaction(msg)
                log_queue_event(msg["transaction_id"], "queued", datetime.now().isoformat())
                return redirect(url_for('index'))
            except Exception as e:
                logger.error(f"Failed to queue transaction: {e}")
                error = "Failed to queue transaction"
                return render_template('index.html', form=form, error=error)

    return render_template('index.html', form=form, success=request.args.get('success'))

@app.route("/login", methods=["GET", "POST"])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        if form.username.data == "aarush" and form.password.data == "Aarush@101109":
            session["logged_in"] = True
            return redirect(url_for("admin"))
        else:
            return render_template('login.html', form=form, error="Invalid credentials")
    return render_template('login.html', form=form)

@app.route("/admin")
def admin():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    
    queue_log = []
    if Path(QUEUE_LOG).exists():
        with open(QUEUE_LOG, 'r') as f:
            queue_log = json.load(f)
    
    transaction_log = []
    if Path(TRANSACTION_LOG).exists():
        with open(TRANSACTION_LOG, 'r') as f:
            transaction_log = json.load(f)
    
    db_tables = get_database_tables()
    
    queue_times = {}
    for event in queue_log:
        txn_id = event["transaction_id"]
        if txn_id not in queue_times:
            queue_times[txn_id] = {}
        queue_times[txn_id][event["event_type"]] = event["timestamp"]
    
    return render_template('admin.html', queue_times=queue_times, transaction_log=transaction_log, db_tables=db_tables)

@app.route("/api/admin-data")
@login_required
def api_admin_data():
    queue_log = []
    if Path(QUEUE_LOG).exists():
        with open(QUEUE_LOG, 'r') as f:
            queue_log = json.load(f)
    
    transaction_log = []
    if Path(TRANSACTION_LOG).exists():
        with open(TRANSACTION_LOG, 'r') as f:
            transaction_log = json.load(f)
    
    db_tables = get_database_tables()
    
    queue_times = []
    queue_dict = {}
    
    for event in queue_log:
        txn_id = event["transaction_id"]
        if txn_id not in queue_dict:
            queue_dict[txn_id] = {}
        queue_dict[txn_id][event["event_type"]] = event["timestamp"]
    
    for txn_id, events in queue_dict.items():
        queue_times.append({
            "transaction_id": txn_id,
            "queued": events.get("queued", "N/A"),
            "delivered": events.get("delivered", None)
        })
    
    return jsonify({
        "queue_times": queue_times,
        "transaction_log": transaction_log,
        "db_tables": db_tables
    })

@app.route("/verify-db")
@login_required
def verify_db():
    conn = get_db_connection()
    if not conn:
        return jsonify({"status": "error", "message": "Cannot connect to database"}), 500
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM transactions;")
        count = cur.fetchone()[0]
        
        cur.execute("SELECT * FROM transactions ORDER BY created_at DESC LIMIT 5;")
        columns = [desc[0] for desc in cur.description]
        recent = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return jsonify({
            "status": "success",
            "total_transactions": count,
            "recent_transactions": recent
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/clear-data", methods=["POST"])
@login_required
def clear_data():
    try:
        if Path(TRANSACTION_LOG).exists():
            Path(TRANSACTION_LOG).unlink()
        if Path(QUEUE_LOG).exists():
            Path(QUEUE_LOG).unlink()
        return jsonify({"status": "success", "message": "Data cleared"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/drop-database", methods=["POST"])
@login_required
def drop_database():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"status": "error", "message": "Cannot connect to database"}), 500
        
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS transactions CASCADE;")
        cur.execute("""
            CREATE TABLE transactions (
                transaction_id UUID PRIMARY KEY,
                amount DECIMAL(10, 2),
                sender VARCHAR(255),
                receiver VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({"status": "success", "message": "Database reset and transactions table recreated"})
    except Exception as e:
        logger.error(f"Database drop failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/logout")
def logout():
    session.pop("logged_in", None)
    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
