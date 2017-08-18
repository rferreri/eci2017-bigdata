
tourists = LOAD 'input/touristData.csv' USING PigStorage(',') AS 
(continent: chararray,
 country: chararray,
 state: chararray,
 wayin: chararray,
 year: int,
 month: chararray,
 count: int
);

-- Discard unnecessary columns
tourists_filtered = FOREACH tourists GENERATE country, count;

-- Group by country and wayin
tourists_grouped = GROUP tourists_filtered BY country;
tourists_by_country = FOREACH tourists_grouped GENERATE group as country, SUM(tourists_filtered.count) as total;
tourists_ordered = ORDER tourists_by_country BY total DESC;

STORE tourists_ordered INTO 'output';