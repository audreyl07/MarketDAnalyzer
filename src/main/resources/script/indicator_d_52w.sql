CREATE TABLE indicator_d_52w (
  type SYMBOL,
  date TIMESTAMP,
  ticker SYMBOL,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  previous_close DOUBLE,
  vol DOUBLE,
  previous_vol DOUBLE,
  high52w DOUBLE,
  previous_high52w DOUBLE,
  high52w_percentage DOUBLE,
  low52w DOUBLE,
  previous_low52w DOUBLE,
  low52w_percentage DOUBLE
),
INDEX(ticker CAPACITY 9000)
TIMESTAMP(date) PARTITION BY YEAR WAL;