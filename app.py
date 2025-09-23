# app.py — tiny API + static hosting for NeuroFinder
import os, math
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import pyodbc

load_dotenv()

def get_cnx():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={os.getenv('SQLSERVER_HOST','127.0.0.1')},{os.getenv('SQLSERVER_PORT','1433')};"
        f"DATABASE={os.getenv('SQLSERVER_DB','master')};"
        f"UID={os.getenv('SQLSERVER_USER')};PWD={os.getenv('SQLSERVER_PASSWORD')};"
        "Encrypt=yes;TrustServerCertificate=yes",
        timeout=5,
    )

def haversine_miles(lat1, lon1, lat2, lon2):
    R = 3958.7613
    dlat = math.radians(lat2-lat1); dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return 2*R*math.asin(min(1, math.sqrt(a)))

app = FastAPI()

@app.get("/api/providers")

def providers(
    q: str = Query("", description="search text"),
    lat: Optional[float] = Query(None),
    lon: Optional[float] = Query(None),
    radius: Optional[float] = Query(50.0),
    limit: int = Query(1000, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    sql = f"""
      SELECT DISTINCT first_name, last_name, address_1, city, state, postal_code
      FROM [Neurology].[stg].[taxonomy] t
      INNER JOIN [Neurology].[stg].[address] a
      ON a.number = t.number
      WHERE a.state IN ('MA','RI')
      AND a.address_purpose = 'LOCATION'
    """
    rows = []
    with get_cnx() as cnx:
        with cnx.cursor() as c:
            c.execute(sql)
            for r in c.fetchall():
                row = dict(zip([d[0] for d in c.description], r))
                row["name"] = (row.pop("first_name","") + " " + row.pop("last_name","")).strip()
                row["addr1"] = row.pop("address_1","")
                row["zip"] = row.pop("postal_code", "")
                rows.append(row)

    # 2) Optional distance compute/filter/sort
    if lat is not None and lon is not None:
        out = []
        for r in rows:
            if r.get("lat") is None or r.get("lon") is None: 
                continue
            try:
                d = haversine_miles(float(r["lat"]), float(r["lon"]), float(lat), float(lon))
            except Exception:
                continue
            if radius is None or d <= float(radius):
                r["distanceMiles"] = d
                out.append(r)
        rows = sorted(out, key=lambda x: x.get("distanceMiles", 1e9))

    total = len(rows)
    items = rows[offset: offset+limit]
    return JSONResponse({"items": items, "total": total})

app.mount("/", StaticFiles(directory="frontend", html=True), name="static")