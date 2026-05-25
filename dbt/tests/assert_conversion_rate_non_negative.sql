-- Conversion rate must never be negative.
SELECT date, conversion_rate
FROM {{ ref('mart_daily_metrics') }}
WHERE conversion_rate < 0
