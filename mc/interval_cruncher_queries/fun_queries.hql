
DROP TABLE IF EXISTS ellery.mc_agg;


CREATE TABLE IF NOT EXISTS ellery.mc_agg (
  curr STRING,
  prev STRING,
  n BIGINT
);


INSERT INTO TABLE ellery.mc_agg
SELECT curr, prev, SUM(n) as n FROM ellery.mc
GROUP BY curr, prev;


SELECT COUNT(*) from ellery.mc_agg; --99,149,487

SELECT SUM(n) from ellery.mc_agg; --1,415,940,274



SELECT * from ellery.mc_agg
WHERE prev = 'Germany'
ORDER BY n DESC
LIMIT 100;


SELECT * from ellery.mc_agg
WHERE prev = 'Ebola_virus_disease'
ORDER BY n DESC
LIMIT 100;
