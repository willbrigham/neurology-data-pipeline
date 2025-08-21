MERGE core.Provider AS tgt
USING (
  SELECT number AS npi,
         MAX(first_name) AS first_name,
         MAX(last_name)  AS last_name,
         MAX(last_updated_ts) AS last_updated_ts
  FROM stg.Taxonomy
  GROUP BY number
) AS src
ON (tgt.npi = src.npi)
WHEN MATCHED AND src.last_updated_ts > tgt.last_updated_ts THEN
  UPDATE SET first_name = src.first_name, last_name = src.last_name, last_updated_ts = src.last_updated_ts
WHEN NOT MATCHED THEN
  INSERT (npi, first_name, last_name, last_updated_ts)
  VALUES (src.npi, src.first_name, src.last_name, src.last_updated_ts);