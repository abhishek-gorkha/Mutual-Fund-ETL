import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

def load_config(path="config/config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def get_engine(cfg):
    db = cfg["database"]
    user = db["user"]
    password = db["password"]
    host = db["host"]
    port = db["port"]
    dbname = db["dbname"]

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

    try:
        engine = create_engine(url)
        return engine
    except SQLAlchemyError as e:
        print("❌ Failed to create engine:", e)
        return None

def test_connection():
    cfg = load_config()
    engine = get_engine(cfg)

    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ PostgreSQL connection successful:", result.scalar())
    except Exception as e:
        print("❌ Connection failed:", e)
