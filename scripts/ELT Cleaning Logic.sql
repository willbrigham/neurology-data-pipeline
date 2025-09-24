USE [Neurology];

GO

/* Ensure schemas */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='bronze') EXEC('CREATE SCHEMA bronze');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='silver') EXEC('CREATE SCHEMA silver');

GO

/* Create and insert into Bronze table */
    IF OBJECT_ID('bronze.tbl_bronze_provider','U') IS NULL
 BEGIN
CREATE TABLE bronze.tbl_bronze_provider (
       npi          VARCHAR(12)    NOT NULL,
       first_name   NVARCHAR(64)   NULL,
       last_name    NVARCHAR(64)   NULL,
       address_1    NVARCHAR(128)  NULL,
       address_2    NVARCHAR(128)  NULL,
       city         NVARCHAR(64)   NULL,
       state        CHAR(2)        NULL,
       postal_code  NVARCHAR(10)   NULL,
       latitude     DECIMAL(9,6)   NULL,
       longitude    DECIMAL(9,6)   NULL,
       ingested_at  DATETIME2(0)   NOT NULL DEFAULT SYSUTCDATETIME()
);
END
ELSE
TRUNCATE TABLE bronze.tbl_bronze_provider;

INSERT INTO bronze.tbl_bronze_provider
 (npi, first_name, last_name, address_1, address_2, city, state, postal_code, latitude, longitude)
SELECT DISTINCT
       t.number        AS npi,
       t.first_name,
       t.last_name,
       a.address_1,
       a.address_2,
       a.city,
       a.state,
       a.postal_code,
       /* keep NULL for now if you don't have coords yet; replace with a.latitude/longitude when available */
      CAST(NULL AS DECIMAL(9,6)) AS latitude,
      CAST(NULL AS DECIMAL(9,6)) AS longitude
 FROM stg.taxonomy t
      INNER JOIN stg.address a
      ON a.number = t.number
WHERE a.address_purpose = 'LOCATION'
  AND t.is_primary = 1;

GO

/* Create Silver Tables and Insert */
IF OBJECT_ID('silver.provider','U') IS NULL
BEGIN
  CREATE TABLE silver.provider (
    npi          VARCHAR(10)    NOT NULL,
    first_name   NVARCHAR(64)   NULL,
    last_name    NVARCHAR(64)   NULL,
    addr1        NVARCHAR(128)  NOT NULL,
    addr2        NVARCHAR(128)  NULL,
    city         NVARCHAR(64)   NOT NULL,
    state        CHAR(2)        NOT NULL,
    zip5         CHAR(5)        NOT NULL,
    address_key  VARCHAR(256)   NOT NULL,  -- canonical key
    lat          DECIMAL(9,6)   NULL,
    lon          DECIMAL(9,6)   NULL,
    source_hash  VARBINARY(32)  NOT NULL,  -- hash of original addr fields (idempotency)
    ingested_at  DATETIME2(0)   NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at   DATETIME2(0)   NULL
  );

  CREATE UNIQUE INDEX UX_silver_provider ON silver.provider(npi, address_key);
  ALTER TABLE silver.provider WITH CHECK
    ADD CONSTRAINT CK_silver_lat CHECK (lat IS NULL OR (lat BETWEEN -90 AND 90)),
        CONSTRAINT CK_silver_lon CHECK (lon IS NULL OR (lon BETWEEN -180 AND 180));
END
GO

;WITH src AS (
  SELECT DISTINCT
    b.npi, b.first_name, b.last_name,
    b.address_1, b.address_2, b.city, b.state, b.postal_code,
    b.latitude, b.longitude
  FROM bronze.tbl_bronze_provider b
  WHERE b.address_1 IS NOT NULL
),
clean AS (
  SELECT
    s.npi,
    s.first_name,
    s.last_name,
    /* address cleaning: trim, uppercase, strip simple punctuation (.,,#) */
    UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(s.address_1, '.', ''), ',', ''), '#', ' ')))) AS addr1_clean,
    CASE WHEN s.address_2 IS NULL THEN NULL
         ELSE UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(s.address_2, '.', ''), ',', ''), '#', ' '))))
    END AS addr2_clean,
    UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(s.city,      '.', ''), ',', ''), '#', ' '))))   AS city_clean,
    UPPER(LTRIM(RTRIM(s.state))) AS state_clean,
    LEFT(LTRIM(RTRIM(s.postal_code)), 5) AS zip5,
    /* carry coords if present in bronze */
    s.latitude  AS lat,
    s.longitude AS lon,
    /* idempotency trigger: hash of original address fields */
    HASHBYTES('SHA2_256', CONCAT_WS('|', s.address_1, s.address_2, s.city, s.state, s.postal_code)) AS source_hash
  FROM src s
),
with_key AS (
  SELECT
    npi, first_name, last_name,
    addr1 = addr1_clean,
    addr2 = addr2_clean,
    city  = city_clean,
    state = state_clean,
    zip5,
    address_key = CONCAT(addr1_clean, '|', city_clean, '|', state_clean, '|', zip5),
    lat, lon, source_hash
  FROM clean
)
MERGE silver.provider AS tgt
USING with_key AS x
  ON tgt.npi = x.npi AND tgt.address_key = x.address_key
WHEN MATCHED AND tgt.source_hash <> x.source_hash THEN
  UPDATE SET
    tgt.first_name  = x.first_name,
    tgt.last_name   = x.last_name,
    tgt.addr1       = x.addr1,
    tgt.addr2       = x.addr2,
    tgt.city        = x.city,
    tgt.state       = x.state,
    tgt.zip5        = x.zip5,
    tgt.lat         = x.lat,
    tgt.lon         = x.lon,
    tgt.source_hash = x.source_hash,
    tgt.updated_at  = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
  INSERT (npi, first_name, last_name, addr1, addr2, city, state, zip5, address_key, lat, lon, source_hash, ingested_at)
  VALUES (x.npi, x.first_name, x.last_name, x.addr1, x.addr2, x.city, x.state, x.zip5, x.address_key, x.lat, x.lon, x.source_hash, SYSUTCDATETIME());
GO