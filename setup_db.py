import psycopg2
from sqlalchemy import create_engine

CONFIG = {
    "DB": {
        "dbname": "dexscreener",
        "user": "admin",
        "password": "Fatsoula.1",
        "host": "localhost",
        "port": "5432"
    }
}

def create_tables():
    engine = create_engine(
        f'postgresql+psycopg2://{CONFIG["DB"]["user"]}:{CONFIG["DB"]["password"]}'
        f'@{CONFIG["DB"]["host"]}/{CONFIG["DB"]["dbname"]}'
    )
    
    with engine.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pairs (
                pair_address VARCHAR(42) PRIMARY KEY,
                base_token_name TEXT,
                base_token_address VARCHAR(42),
                quote_token_address VARCHAR(42),
                price FLOAT,
                liquidity FLOAT,
                volume_24h FLOAT,
                chain TEXT,
                exchange TEXT,
                created_at TIMESTAMP,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

if __name__ == "__main__":
    create_tables()
    print("Database tables created successfully.")
