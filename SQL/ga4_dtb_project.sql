-- raw events
SELECT *
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
;
-- explore event_params keys by event_name
SELECT
  event_name,
  ep.key,
  ANY_VALUE(ep.value.string_value)  AS example_string,
  ANY_VALUE(ep.value.int_value)     AS example_int,
  ANY_VALUE(ep.value.double_value)  AS example_double
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
UNNEST(event_params) AS ep
GROUP BY event_name, ep.key
ORDER BY event_name, ep.key
;
-- stg_events: flatten key params at event level
CREATE OR REPLACE VIEW `md-ga4-marketing-analysis.ga4_campaign_data.stg_events` AS
SELECT
  PARSE_DATE('%Y%m%d', event_date)                        AS event_date,
  TIMESTAMP_MICROS(event_timestamp)                       AS event_ts,
  event_name,
  user_pseudo_id,
  (SELECT value.int_value    FROM UNNEST(event_params) WHERE key = "ga_session_id")      AS ga_session_id,
  (SELECT value.int_value    FROM UNNEST(event_params) WHERE key = "ga_session_number")  AS ga_session_number,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_location")      AS page_location,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "campaign")           AS campaign,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "source")             AS source,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "medium")             AS medium,
  (SELECT value.int_value    FROM UNNEST(event_params) WHERE key = "engagement_time_msec") AS engagement_time_msec,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "transaction_id")     AS transaction_id,
  (SELECT value.int_value    FROM UNNEST(event_params) WHERE key = "value")              AS event_value,
  device.category    AS device_category,
  device.operating_system,
  geo.country        AS country,
  geo.region         AS region,
  geo.city           AS city,
  platform
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
;
-- stg_sessions: flatten key params at session level
CREATE OR REPLACE VIEW `md-ga4-marketing-analysis.ga4_campaign_data.stg_sessions` AS
WITH base AS (
  SELECT
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "ga_session_id") AS ga_session_id,
    MIN(TIMESTAMP_MICROS(event_timestamp)) AS session_start_ts,
    MAX(TIMESTAMP_MICROS(event_timestamp)) AS session_end_ts,
    COUNT(*) AS events_in_session,
    COUNTIF(event_name = 'page_view') AS pageviews,
    COUNTIF(event_name = 'purchase')  AS purchases,
    ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = "source"))   AS session_source,
    ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = "medium"))   AS session_medium,
    ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = "campaign")) AS session_campaign,
    ANY_VALUE(device.category) AS device_category,
    ANY_VALUE(geo.country)     AS country
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  GROUP BY user_pseudo_id, ga_session_id
)
SELECT
  *,
  TIMESTAMP_DIFF(session_end_ts, session_start_ts, SECOND) AS session_duration_sec
FROM base
WHERE ga_session_id IS NOT NULL
;
-- stg_purchases: flatten key params at purchase level
CREATE OR REPLACE VIEW `md-ga4-marketing-analysis.ga4_campaign_data.stg_purchases` AS
SELECT
  PARSE_DATE('%Y%m%d', e.event_date)                   AS event_date,
  TIMESTAMP_MICROS(e.event_timestamp)                  AS purchase_ts,
  e.user_pseudo_id,
  (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = "ga_session_id") AS ga_session_id,
  (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = "transaction_id") AS transaction_id,
  i.item_id,
  i.item_name,
  i.item_brand,
  i.item_category,
  i.price,
  i.quantity,
  (i.price * i.quantity)                               AS line_revenue,
  (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = "currency") AS currency,
  (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = "source")   AS source,
  (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = "medium")   AS medium,
  (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = "campaign") AS campaign
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS e
LEFT JOIN UNNEST(e.items) AS i
WHERE e.event_name = 'purchase'
;
-- stg_traffic_touches
CREATE OR REPLACE VIEW `md-ga4-marketing-analysis.ga4_campaign_data.stg_traffic_touches` AS
SELECT
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  TIMESTAMP_MICROS(event_timestamp) AS event_ts,
  user_pseudo_id,
  (SELECT value.int_value    FROM UNNEST(event_params) WHERE key = "ga_session_id") AS ga_session_id,
  event_name,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "source")   AS source,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "medium")   AS medium,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "campaign") AS campaign
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE EXISTS (
  SELECT 1 FROM UNNEST(event_params) ep
  WHERE ep.key IN ('source','medium','campaign')
)
;
-- =====================================================================
-- FACT: Campaign Performance (Daily × Channel Grain)
-- Project: md-ga4-marketing-analysis | Dataset: ga4_campaign_data
-- Grain: 1 row per event_date × source × medium × campaign
-- =====================================================================

CREATE OR REPLACE TABLE `md-ga4-marketing-analysis.ga4_campaign_data.fact_campaign_performance`
PARTITION BY event_date
CLUSTER BY source, medium, campaign
AS
-- Build metric sets from staging
WITH
ev AS (
  -- Event-level metrics: impressions & clicks
  SELECT
    event_date,
    source,
    medium,
    campaign,
    COUNTIF(event_name = 'page_view') AS impressions,
    COUNTIF(event_name = 'click')     AS clicks
  FROM `md-ga4-marketing-analysis.ga4_campaign_data.stg_events`
  GROUP BY 1,2,3,4
),
sess AS (
  -- Session-level metrics: visits
  SELECT
    DATE(session_start_ts) AS event_date,
    session_source         AS source,
    session_medium         AS medium,
    session_campaign       AS campaign,
    COUNT(*)               AS sessions
  FROM `md-ga4-marketing-analysis.ga4_campaign_data.stg_sessions`
  GROUP BY 1,2,3,4
),
tx AS (
  -- Transaction-level metrics: conversions & revenue
  SELECT
    event_date,
    source,
    medium,
    campaign,

    -- Strict: only non-null transaction IDs
    COUNT(DISTINCT transaction_id) AS conversions,

    -- Inclusive: give NULL transaction_id a synthetic ID based on user + purchase timestamp
    COUNT(DISTINCT COALESCE(
      transaction_id,
      CONCAT('missing#', CAST(user_pseudo_id AS STRING), '#', CAST(purchase_ts AS STRING))
    )) AS conversions_incl_null,

    SUM(line_revenue) AS revenue
  FROM `md-ga4-marketing-analysis.ga4_campaign_data.stg_purchases`
  GROUP BY 1,2,3,4
)

-- Merge all three sets at the common grain
SELECT
  COALESCE(ev.event_date, sess.event_date, tx.event_date) AS event_date,
  COALESCE(ev.source,     sess.source,     tx.source)     AS source,
  COALESCE(ev.medium,     sess.medium,     tx.medium)     AS medium,
  COALESCE(ev.campaign,   sess.campaign,   tx.campaign)   AS campaign,

  -- Base metrics (zero-filled where missing)
  IFNULL(ev.impressions, 0)           AS impressions,
  IFNULL(ev.clicks, 0)                AS clicks,
  IFNULL(sess.sessions, 0)            AS sessions,
  IFNULL(tx.conversions, 0)           AS conversions,             -- strict (default)
  IFNULL(tx.conversions_incl_null, 0) AS conversions_incl_null,   -- inclusive (reference)
  IFNULL(tx.revenue, 0.0)             AS revenue,

  -- Derived KPIs
  SAFE_DIVIDE(ev.clicks, ev.impressions)           AS ctr,            -- Click-through rate
  SAFE_DIVIDE(tx.conversions, sess.sessions)       AS cvr,            -- Session → purchase (strict)
  SAFE_DIVIDE(tx.revenue, tx.conversions)          AS aov,            -- AOV (strict)
  SAFE_DIVIDE(tx.conversions, sess.sessions)       AS session_to_purchase_rate,
  SAFE_DIVIDE(tx.conversions_incl_null, sess.sessions) AS cvr_incl_null,  -- optional
  SAFE_DIVIDE(tx.revenue, conversions_incl_null)       AS aov_incl_null   -- optional

FROM ev
FULL OUTER JOIN sess
  ON  ev.event_date = sess.event_date
  AND ev.source     = sess.source
  AND ev.medium     = sess.medium
  AND ev.campaign   = sess.campaign
FULL OUTER JOIN tx
  ON  COALESCE(ev.event_date, sess.event_date) = tx.event_date
  AND COALESCE(ev.source,     sess.source)     = tx.source
  AND COALESCE(ev.medium,     sess.medium)     = tx.medium
  AND COALESCE(ev.campaign,   sess.campaign)   = tx.campaign
;


-- dim_customers (user grain)
CREATE OR REPLACE VIEW `md-ga4-marketing-analysis.ga4_campaign_data.dim_customers` AS
WITH activity AS (
  SELECT
    user_pseudo_id,
    MIN(DATE(session_start_ts)) AS first_seen_date,
    MAX(DATE(session_end_ts))   AS last_seen_date,
    COUNT(*)                    AS sessions_count
  FROM `md-ga4-marketing-analysis.ga4_campaign_data.stg_sessions`
  GROUP BY 1
),
events_totals AS (
  SELECT
    user_pseudo_id,
    COUNT(*) AS total_events
  FROM `md-ga4-marketing-analysis.ga4_campaign_data.stg_events`
  GROUP BY 1
),
purch AS (
  SELECT
    user_pseudo_id,
    COUNT(DISTINCT transaction_id) AS purchases_count,
    COALESCE(SUM(line_revenue), 0.0) AS total_revenue
  FROM `md-ga4-marketing-analysis.ga4_campaign_data.stg_purchases`
  GROUP BY 1
),
att AS (
  -- representative attributes; swap to "latest by timestamp" later if you want
  SELECT
    user_pseudo_id,
    ANY_VALUE(device_category) AS device_category,
    ANY_VALUE(country)         AS country,
    ANY_VALUE(platform)        AS platform
  FROM `md-ga4-marketing-analysis.ga4_campaign_data.stg_events`
  GROUP BY 1
)
SELECT
  a.user_pseudo_id,
  att.device_category,
  att.country,
  att.platform,

  a.first_seen_date,
  a.last_seen_date,
  DATE_DIFF(CURRENT_DATE(), a.last_seen_date, DAY) AS recency_days,
  a.sessions_count,
  et.total_events,

  COALESCE(p.purchases_count, 0) AS purchases_count,
  COALESCE(p.total_revenue,  0.0) AS total_revenue,
  SAFE_DIVIDE(p.total_revenue, NULLIF(p.purchases_count,0)) AS avg_order_value,
  SAFE_DIVIDE(p.purchases_count, NULLIF(a.sessions_count,0)) AS purchase_per_session
FROM activity a
LEFT JOIN events_totals et USING (user_pseudo_id)
LEFT JOIN purch         p  USING (user_pseudo_id)
LEFT JOIN att              USING (user_pseudo_id)
;
