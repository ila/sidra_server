CREATE DECENTRALIZED TABLE home_energy_logs (
  device_id VARCHAR SENSITIVE,
  timestamp TIMESTAMPTZ,
  indoor_temp DECIMAL FACT,
  outdoor_temp DECIMAL,
  heating_on BOOLEAN,
  energy_used_kwh DECIMAL FACT,
  postal_code VARCHAR DIMENSION
);

CREATE DECENTRALIZED MATERIALIZED VIEW daily_energy_usage_user
AS (
  SELECT
    DATE(timestamp) AS day,
    postal_code,
    SUM(energy_used_kwh) AS total_kwh,
    AVG(indoor_temp) AS avg_temp
  FROM home_energy_logs
  WHERE heating_on = TRUE
  GROUP BY day, postal_code
)
WINDOW 24
TTL 72
REFRESH 6;

CREATE CENTRALIZED MATERIALIZED VIEW daily_energy_by_postal_code
AS (
  SELECT
    postal_code,
    day,
    SUM(total_kwh) AS total_kwh,
    AVG(avg_temp) AS mean_indoor_temp
  FROM daily_energy_usage_user
  GROUP BY postal_code, day
)
REFRESH 12
MINIMUM AGGREGATION 15;

CREATE REPLICATED MATERIALIZED VIEW local_weather_feedback
AS (
  SELECT
    postal_code,
    AVG(outdoor_temp) AS avg_outdoor_temp,
    COUNT(*) AS readings_count
  FROM home_energy_logs
  GROUP BY postal_code
)
REFRESH 24;

CREATE CENTRALIZED TABLE region_metadata (
  postal_code VARCHAR PRIMARY KEY,
  city_name VARCHAR,
  climate_zone VARCHAR,
  energy_price_per_kwh DECIMAL
);

pragma generate_server_refresh_script('home_sensor');
