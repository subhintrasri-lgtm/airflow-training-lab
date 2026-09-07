import json
import re
import sqlite3
from pathlib import Path


database_path = Path(__file__).with_name("database_snapshot.sqlite")
connection = sqlite3.connect(database_path)
connection.row_factory = sqlite3.Row

tables = connection.execute(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'execution%'"
).fetchall()
print("tables:", [row["name"] for row in tables])

rows = connection.execute(
    "SELECT id, status, workflowId, startedAt, stoppedAt "
    "FROM execution_entity ORDER BY id DESC LIMIT 8"
).fetchall()
print("recent executions:")
for row in rows:
    print(dict(row))

latest = connection.execute(
    "SELECT e.id, e.status, d.data "
    "FROM execution_entity AS e "
    "JOIN execution_data AS d ON d.executionId = e.id "
    "WHERE e.workflowId = ? ORDER BY e.id DESC LIMIT 1",
    ("MraAirflowWebhook01",),
).fetchone()

if latest:
    print("latest workflow execution:", latest["id"], latest["status"])
    raw = latest["data"]
    print("data type/length:", type(raw).__name__, len(raw) if raw else 0)
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    print("contains Get_Airflow_Log:", "Get_Airflow_Log" in (raw or ""))
    print("contains supplyData error:", "supplyData" in (raw or ""))
    api_paths = sorted(
        {
            value.replace("\\/", "/")
            for value in re.findall(
                r"dags/mra_agentic_pipeline[^\"\\]*(?:\\.[^\"\\]*)*", raw or ""
            )
            if len(value) < 500
        }
    )
    print("Airflow API paths used:")
    for api_path in api_paths:
        print(" -", api_path)
    Path(__file__).with_name("latest_execution_data.txt").write_text(
        raw or "", encoding="utf-8"
    )

connection.close()
