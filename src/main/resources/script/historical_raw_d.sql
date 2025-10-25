CREATE TABLE historical_raw_d (
  ticker SYMBOL CAPACITY 9000 CACHE,
  per SYMBOL CAPACITY 2 CACHE,
  date VARCHAR,
  time VARCHAR,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  vol DOUBLE,
  openint INT
);