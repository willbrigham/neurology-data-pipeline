SELECT TOP 5 * FROM stg.Taxonomy;
SELECT TOP 5 * FROM stg.Addresses;

-- Counts
SELECT COUNT(*) AS tax_cnt FROM stg.Taxonomy;
SELECT COUNT(*) AS addr_cnt FROM stg.Addresses;

-- Neurologists present?
SELECT COUNT(*) FROM stg.Taxonomy WHERE taxonomy_code='2084N0400X';