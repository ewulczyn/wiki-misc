CREATE TABLE IF NOT EXISTS ellery.oozie_mc (
  prev STRING,
  curr STRING,
  n BIGINT
)
PARTITIONED BY (year INT, month INT, day INT);