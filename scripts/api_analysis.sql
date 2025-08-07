-- I found my neurologist! YAY!
SELECT *
  FROM addresses a
       INNER JOIN taxonomy t
       ON t.number = a.number
 WHERE first_name LIKE 'SIM%'

-- What states are these neurologists from?
-- I am expecting only 'MA', why are there 35 distinct states
-- Need to check API to get correct practice locations
-- Maybe check that each number has at last one address in MA
SELECT DISTINCT(a.state)
  FROM addresses a

-- Check to see how many records appear for Simona Nedelcu (random example)
SELECT *
  FROM taxonomy t
 WHERE number = 1437689833

SELECT t.number
  FROM taxonomy t
