"""Export mart.v_analytics_master to CSV for use in Colab notebooks.

Run from project root:
    python scripts/export_for_colab.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

engine = create_engine(
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@localhost:5432/{os.getenv('POSTGRES_DB', 'bkk_analytics')}"
)

df = pd.read_sql("SELECT * FROM mart.v_analytics_master", engine)
df.to_csv("data/analytics_master.csv", index=False)
print(f"Exported {len(df):,} rows to data/analytics_master.csv")
