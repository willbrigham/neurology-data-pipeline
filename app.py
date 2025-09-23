# app.py — tiny API + static hosting for NeuroFinder
import os, math
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import pyodbc

load_dotenv()

CNX = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=Neurology;"
    "UID=sa;PWD=YourStrong!Passw0rd;"
    "Encrypt=yes;TrustServerCertificate=yes"
)

def haversine_miles(lat1, lon1, lat2, lon2):
    R = 3958.7613
    dlat = math.radians(lat2-lat1); dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return 2*R*math.asin(min(1, math.sqrt(a)))

app = FastAPI()

# Serve your frontend/ folder as the site root
app.mount("/", StaticFiles(directory="frontend", html=True), name="static")

@app.get("/api/providers")
def providers(
    q: str = Query("", description="search text"),
    lat: Optional[float] = Query(None),
    lon: Optional[float] = Query(None),
    radius: Optional[float] = Query(50.0),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    # 1) Pull a reasonable slice from SQL Server
    like = f"%{q.strip()}%" if q else "%"
    cols = "provider_id, first_name, last_name, org_name, addr1, city, state, zip, lat, lon"
    sql = f"""
      SELECT TOP (1000) {cols}
      FROM silver.provider
      WHERE state IN ('MA','RI')
        AND (
          (? = '%' ) OR
          (CONCAT(first_name,' ',last_name) LIKE ?) OR
          (org_name LIKE ?) OR
          (city LIKE ?)
        )
    """
    rows = []
    with CNX.cursor() as c:
        c.execute(sql, (like, like, like, like))
        for r in c.fetchall():
            row = dict(zip([d[0] for d in c.description], r))
            row["name"] = (row.pop("first_name","") + " " + row.pop("last_name","")).strip()
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
