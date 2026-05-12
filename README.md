# S&P 500 Real-Time Stock Market Streaming Pipeline

## Overview
Built an end-to-end real-time streaming pipeline that ingests live S&P 500 
stock market data using Python and streams it into Microsoft Fabric KQL 
Database for real-time analytics and monitoring.

## Architecture
```
Python (yfinance) → azure-kusto-ingest → KQL Database (Fabric) → Real-Time Dashboard
```

## Tech Stack
| Tool | Purpose |
|------|---------|
| Python + yfinance | Fetch real S&P 500 stock prices |
| azure-kusto-ingest | Stream data into Microsoft Fabric |
| Microsoft Fabric KQL Database | Real-time storage & queries |
| KQL (Kusto Query Language) | Analytics, trends, anomaly detection |
| Fabric Real-Time Dashboard | Live auto-refreshing visuals |

## Tickers Tracked
AAPL · MSFT · GOOGL · AMZN · NVDA · META · TSLA · JPM · UNH · BRK-B

## Key Features
- Ingests real live S&P 500 stock prices every 60 seconds
- BUY / SELL / HOLD signal generation based on price movement
- Anomaly detection for price spikes above 2% change
- Auto-refreshing Real-Time Dashboard every 30 seconds
- KQL queries for trend analysis, top movers, rolling averages

## KQL Highlights
```kql
// Latest price per ticker
stock_prices
| summarize arg_max(timestamp, *) by ticker
| project ticker, price, change_pct, signal
| order by change_pct desc

// Anomaly detection
stock_prices
| where abs(change_pct) > 2.0
| project ticker, price, change_pct, signal, timestamp
| order by abs(change_pct) desc
```

## Dashboard Preview
![Dashboard](screenshots/Dashboard.png)

## Pipeline Flow
![Pipeline](screenshots/eventstream.png)

## How to Run
```bash
# Install dependencies
pip install yfinance azure-kusto-data azure-kusto-ingest pandas

# Update INGEST_URI in stock_stream.py with your Fabric KQL ingestion URI

# Run the pipeline
python stock_stream.py
```

## Results
- 10 S&P 500 companies tracked in real-time
- Live BUY/SELL/HOLD signals generated every 60 seconds
- Real-Time Dashboard with 4 auto-refreshing visuals
