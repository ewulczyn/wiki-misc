DROP TABLE IF EXISTS ellery.oozie_impressions;
CREATE TABLE IF NOT EXISTS  ellery.oozie_impressions (
  anonymous STRING,
  banner STRING,
  campaign STRING,
  country STRING,
  device STRING,
  minute STRING,
  project STRING,
  reason STRING,
  result STRING,
  uselang STRING,
  db STRING,
  bucket INT,
  spider BOOLEAN,
  n INT
)
PARTITIONED BY (year INT, month INT, day INT);