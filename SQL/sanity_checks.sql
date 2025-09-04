-- =====================================================================
-- GA4 FACT/DIM VALIDATION (no TEMP objects)
-- Creates two helper tables, runs checks, then drops helpers.
-- Project: md-ga4-marketing-analysis | Dataset: ga4_campaign_data
-- =====================================================================

-- 0) Build helper tables (scoped to your dataset)
CREATE OR REPLACE TABLE `md-ga4-marketing-analysis.ga4_campaign_data._fact_daily_check` AS
SELECT
  event_date,
  SUM(impressions) AS impressions,
  SUM(clicks)      AS clicks,
  SUM(sessions)    AS sessions,
  SUM(conversions) AS conversions,
  SUM(revenue)     AS revenue
FROM `md-ga4-marketing-analysis.ga4_campaign_data.fact_campaign_performance`
GROUP BY 1;

CREATE OR REPLACE TABLE `md-ga4-marketing-analysis.ga4_campaign_data._stg_truth_daily_check` AS
WITH ev AS (
  SELECT
    event_date,
    COUNTIF(event_name = 'page_view') AS impressions,
    COUNTIF(event_name = 'click')     AS clicks
  FROM `md-ga4-marketing-analysis.ga4_campaign_data.stg_events`
  GROUP BY 1
),
sess AS (
  SELECT
    DATE(session_start_ts) AS event_date,
    COUNT(*)               AS sessions
  FROM `md-ga4-marketing-analysis.ga4_campaign_data.stg_sessions`
  GROUP BY 1
),
tx AS (
  SELECT
    event_date,
    COUNT(DISTINCT transaction_id) AS conversions,
    SUM(line_revenue)              AS revenue
  FROM `md-ga4-marketing-analysis.ga4_campaign_data.stg_purchases`
  GROUP BY 1
)
SELECT
  COALESCE(ev.event_date, sess.event_date, tx.event_date) AS event_date,
  IFNULL(ev.impressions,0) AS impressions,
  IFNULL(ev.clicks,0)      AS clicks,
  IFNULL(sess.sessions,0)  AS sessions,
  IFNULL(tx.conversions,0) AS conversions,
  IFNULL(tx.revenue,0)     AS revenue
FROM ev
FULL OUTER JOIN sess USING(event_date)
FULL OUTER JOIN tx   USING(event_date);

-- 1) Reconciliation — EXPECT: zero rows
SELECT
  f.event_date,
  f.impressions AS fact_impressions,
  t.impressions AS stg_impressions,
  f.clicks      AS fact_clicks,
  t.clicks      AS stg_clicks,
  f.sessions    AS fact_sessions,
  t.sessions    AS stg_sessions,
  f.conversions AS fact_conversions,
  t.conversions AS stg_conversions,
  f.revenue     AS fact_revenue,
  t.revenue     AS stg_revenue
FROM `md-ga4-marketing-analysis.ga4_campaign_data._fact_daily_check` f
FULL OUTER JOIN `md-ga4-marketing-analysis.ga4_campaign_data._stg_truth_daily_check` t USING(event_date)
WHERE NOT (
  IFNULL(f.impressions,0) = IFNULL(t.impressions,0) AND
  IFNULL(f.clicks,0)      = IFNULL(t.clicks,0) AND
  IFNULL(f.sessions,0)    = IFNULL(t.sessions,0) AND
  IFNULL(f.conversions,0) = IFNULL(t.conversions,0) AND
  ROUND(IFNULL(f.revenue,0), 2) = ROUND(IFNULL(t.revenue,0), 2)
)
ORDER BY event_date;

-- 2) KPI bounds — EXPECT: zero rows
SELECT *
FROM `md-ga4-marketing-analysis.ga4_campaign_data.fact_campaign_performance`
WHERE (ctr  < 0 OR ctr  > 1)
   OR (cvr  < 0 OR cvr  > 1)
   OR (conversions > 0 AND (aov IS NULL OR aov <= 0))
LIMIT 100;

-- 3) No negative metrics — EXPECT: zero rows
SELECT *
FROM `md-ga4-marketing-analysis.ga4_campaign_data.fact_campaign_performance`
WHERE impressions < 0 OR clicks < 0 OR sessions < 0
   OR conversions < 0 OR revenue < 0
LIMIT 100;

-- 4) Duplicate / multi-day transactions — EXPECT: zero rows
SELECT
  transaction_id,
  COUNT(DISTINCT event_date) AS distinct_dates
FROM `md-ga4-marketing-analysis.ga4_campaign_data.stg_purchases`
GROUP BY 1
HAVING COUNT(DISTINCT event_date) > 1
ORDER BY distinct_dates DESC
LIMIT 100;

-- 5) Null/blank channel labels — EXPECT: low counts
SELECT
  COUNT(*) AS rows_in_fact,
  COUNTIF(source   IS NULL OR source   = '') AS null_source,
  COUNTIF(medium   IS NULL OR medium   = '') AS null_medium,
  COUNTIF(campaign IS NULL OR campaign = '') AS null_campaign
FROM `md-ga4-marketing-analysis.ga4_campaign_data.fact_campaign_performance`;

-- 6) Conversions should not exceed sessions — EXPECT: zero or very few
SELECT *
FROM `md-ga4-marketing-analysis.ga4_campaign_data.fact_campaign_performance`
WHERE conversions > sessions
ORDER BY event_date DESC
LIMIT 100;

-- 7) Fact completeness — EXPECT: zero rows
WITH days_with_events AS (
  SELECT DISTINCT event_date FROM `md-ga4-marketing-analysis.ga4_campaign_data.stg_events`
),
days_in_fact AS (
  SELECT DISTINCT event_date FROM `md-ga4-marketing-analysis.ga4_campaign_data.fact_campaign_performance`
)
SELECT e.event_date
FROM days_with_events e
LEFT JOIN days_in_fact f USING(event_date)
WHERE f.event_date IS NULL
ORDER BY e.event_date DESC;

-- 8) dim_customers population & nulls — EXPECT: modest nulls
SELECT
  COUNT(*) AS users,
  COUNTIF(device_category IS NULL) AS null_device,
  COUNTIF(country IS NULL)         AS null_country,
  COUNTIF(platform IS NULL)        AS null_platform
FROM `md-ga4-marketing-analysis.ga4_campaign_data.dim_customers`;

-- 9) RFM-lite sanity — EXPECT: all zeros
SELECT
  COUNTIF(recency_days < 0)    AS negative_recency,
  COUNTIF(purchases_count < 0) AS negative_purchases,
  COUNTIF(total_revenue  < 0)  AS negative_revenue
FROM `md-ga4-marketing-analysis.ga4_campaign_data.dim_customers`;

-- 10) Touchpoint readiness — EXPECT: touches > 0; empty_labels = 0
SELECT
  COUNT(*) AS touches,
  COUNTIF(source IS NULL AND medium IS NULL AND campaign IS NULL) AS empty_labels
FROM `md-ga4-marketing-analysis.ga4_campaign_data.stg_traffic_touches`;

-- 11) Optional cleanup (remove helper tables)
DROP TABLE `md-ga4-marketing-analysis.ga4_campaign_data._fact_daily_check`;
DROP TABLE `md-ga4-marketing-analysis.ga4_campaign_data._stg_truth_daily_check`;