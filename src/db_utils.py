import os
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ------------------------------------------------------------
# Load configuration from YAML
# ------------------------------------------------------------
def load_config(path="config/config.yaml"):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)

    # Ensure database URL exists
    db = cfg.get("database", {})
    if "url" not in db:
        user = db.get("user")
        password = db.get("password")
        host = db.get("host", "localhost")
        port = db.get("port", 5432)
        dbname = db.get("dbname")
        cfg["database"]["url"] = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    
    return cfg

# ------------------------------------------------------------
# Create SQLAlchemy engine
# ------------------------------------------------------------
def get_engine(cfg):
    db_url = cfg["database"]["url"]
    try:
        engine = create_engine(db_url)
        return engine
    except SQLAlchemyError as e:
        print("❌ Failed to create engine:", e)
        return None

# ------------------------------------------------------------
# Test PostgreSQL connection
# ------------------------------------------------------------
def test_connection():
    cfg = load_config()
    engine = get_engine(cfg)
    
    if engine is None:
        print("❌ Engine creation failed. Cannot test connection.")
        return
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ PostgreSQL connection successful:", result.scalar())
    except Exception as e:
        print("❌ Connection failed:", e)

# ------------------------------------------------------------
# Helper: safe execution of a query
# ------------------------------------------------------------
def execute_query(query, engine):
    """
    Execute a SQL query safely.
    Returns True if successful, False if error occurs.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()
        return True
    except SQLAlchemyError as e:
        print(f"❌ Query execution failed: {e}")
        return False
