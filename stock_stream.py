from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties
import yfinance as yf
import pandas as pd
from datetime import datetime
import time

# Your Fabric KQL Ingestion URI
INGEST_URI = "https://ingest-trd-274ud9f08dkvkjmq4b.z0.kusto.fabric.microsoft.com"
DATABASE = "StreamingEventhouse"
TABLE = "stock_prices"

# Top 10 S&P 500 companies
TICKERS = ["AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","JPM","UNH","BRK-B"]

def get_stock_data():
    records = []
    data = yf.download(TICKERS, period="1d", interval="1m", progress=False, auto_adjust=True)
    for ticker in TICKERS:
        try:
            close_price = float(data["Close"][ticker].dropna().iloc[-1])
            open_price  = float(data["Open"][ticker].dropna().iloc[-1])
            volume      = int(data["Volume"][ticker].dropna().iloc[-1])
            change_pct  = round((close_price - open_price) / open_price * 100, 4)
            records.append({
                "ticker": ticker,
                "price": round(close_price, 2),
                "open": round(open_price, 2),
                "volume": volume,
                "change_pct": change_pct,
                "signal": "BUY" if change_pct > 0.5 else "SELL" if change_pct < -0.5 else "HOLD",
                "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
            })
        except Exception as e:
            print(f"Skipping {ticker}: {e}")
    return records

def ingest_to_fabric(records):
    kcsb = KustoConnectionStringBuilder.with_interactive_login(INGEST_URI)
    ingest_client = QueuedIngestClient(kcsb)
    ingestion_props = IngestionProperties(database=DATABASE, table=TABLE)
    
    df = pd.DataFrame(records)
    from azure.kusto.ingest import FileDescriptor
    import tempfile, os
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
        df.to_csv(f, index=False)
        tmp_path = f.name
    
    ingest_client.ingest_from_file(tmp_path, ingestion_properties=ingestion_props)
    os.unlink(tmp_path)
    print(f"[{datetime.utcnow()}] Ingested {len(records)} records to Fabric KQL")

# Main loop
while True:
    try:
        print("Fetching S&P 500 data...")
        records = get_stock_data()
        if records:
            print(f"Got {len(records)} stocks — sending to Fabric...")
            ingest_to_fabric(records)
    except Exception as e:
        print(f"Error: {e}")
    print("Waiting 60 seconds...")
    time.sleep(60)