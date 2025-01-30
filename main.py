import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine
from sklearn.ensemble import IsolationForest

# Configuration
CONFIG = {
    "DB": {
        "dbname": "dexscreener",
        "user": "admin",
        "password": "Fatsoula.1",
        "host": "localhost",
        "port": "5432"
    },
    "FILTERS": {
        "min_liquidity": 5000,
        "min_age_days": 3,
        "chain_whitelist": ["ethereum", "binance-smart-chain"]
    }
}

class EnhancedDexScreenerBot:
    def __init__(self):
        self.engine = create_engine(
            f'postgresql+psycopg2://{CONFIG["DB"]["user"]}:{CONFIG["DB"]["password"]}'
            f'@{CONFIG["DB"]["host"]}/{CONFIG["DB"]["dbname"]}'
        )
        self.model = IsolationForest(n_estimators=100, contamination=0.01)
        self.historical_data = pd.DataFrame()

    def apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['chain'].isin(CONFIG["FILTERS"]["chain_whitelist"])]
        df = df[df['liquidity'] >= CONFIG["FILTERS"]["min_liquidity"]]
        min_age = datetime.utcnow() - timedelta(days=CONFIG["FILTERS"]["min_age_days"])
        df = df[pd.to_datetime(df['created_at']) < min_age]
        return df

    def process_data(self, raw_data):
        df = pd.DataFrame(raw_data)
        df['created_at'] = pd.to_datetime(df['created_at'], unit='ms')
        df['timestamp'] = datetime.utcnow()
        df = self.apply_filters(df)
        return df

    def detect_anomalies(self, new_data: pd.DataFrame) -> pd.DataFrame:
        if not new_data.empty:
            features = new_data[['price', 'liquidity', 'volume_24h']].apply(np.log1p)
            self.model.fit(self.historical_data[['price', 'liquidity', 'volume_24h']])
            anomalies = self.model.predict(features)
            new_data['anomaly_score'] = self.model.decision_function(features)
            return new_data[anomalies == -1]
        return pd.DataFrame()

    def run(self):
        while True:
            try:
                raw_data = self.fetch_pair_data()
                processed_data = self.process_data(raw_data)
                anomalies = self.detect_anomalies(processed_data)

                if not processed_data.empty:
                    processed_data.to_sql('pairs', self.engine, if_exists='append', index=False)
                    self.historical_data = pd.concat([self.historical_data, processed_data]).tail(100000)

                time.sleep(60)
            except Exception as e:
                print(f"Runtime error: {e}")

if __name__ == "__main__":
    bot = EnhancedDexScreenerBot()
    bot.run()
