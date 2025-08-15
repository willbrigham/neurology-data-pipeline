-- 1) Create schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

-- 2) Drop/recreate staging tables (safe to overwrite; staging is reloadable)
IF OBJECT_ID('stg.taxonomy') IS NOT NULL DROP TABLE stg.taxonomy;
IF OBJECT_ID('stg.address') IS NOT NULL DROP TABLE stg.address;
GO

-- 3) Staging: Taxonomy (from df_tax)
CREATE TABLE stg.taxonomy (
    number         varchar(20)  NOT NULL,  -- NPI
    first_name     varchar(200) NULL,
    last_name      varchar(200) NULL,
    taxonomy_code  varchar(20)  NULL,      -- e.g., 2084N0400X
    specialty      varchar(255) NULL,      -- human-readable desc
    is_primary     bit          NULL
);
GO

-- Helpful indexes for joins and filters
CREATE INDEX IX_stg_taxonomy_number         ON stg.taxonomy(number);
CREATE INDEX IX_stg_taxonomy_taxcode_npi    ON stg.taxonomy(taxonomy_code, number);
GO

-- 4) Staging: Address (from df_addr)
CREATE TABLE stg.address (
    number           varchar(20)  NOT NULL,  -- NPI
    address_1        varchar(255) NULL,
    address_2        varchar(255) NULL,
    city             varchar(120) NULL,
    state            varchar(10)  NULL,
    postal_code      varchar(20)  NULL,
    address_purpose  varchar(20)  NULL       -- LOCATION, MAILING
);
GO

-- Helpful indexes for geo lookups
CREATE INDEX IX_stg_address_number        ON stg.address(number);
CREATE INDEX IX_stg_address_state_city    ON stg.address(state, city);
CREATE INDEX IX_stg_address_purpose       ON stg.address(address_purpose);
GO