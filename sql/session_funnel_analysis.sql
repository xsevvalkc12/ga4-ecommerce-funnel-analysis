-- GA4 E-commerce Session Funnel Analysis
-- Author: Sevval Öztürk
-- Description:
-- This query builds a session-based funnel from session_start to purchase
-- using GA4 public dataset in BigQuery.
-- NOTE:
-- Table name can be adjusted based on the user's BigQuery project and dataset

CREATE OR REPLACE TABLE `myproject1-476715.ecommerce_funnel_p1.session_funnel` AS
WITH base AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_dt,
    event_timestamp,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    event_name,

    -- landing page için session_start anındaki page_location
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,

    device.category AS device_category,
    device.language AS device_language,
    device.operating_system AS operating_system,

    traffic_source.source AS source,
    traffic_source.medium AS medium,
    traffic_source.name   AS campaign,

    ecommerce.purchase_revenue_in_usd AS revenue_usd,
    ecommerce.transaction_id AS transaction_id

  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
),
session_keyed AS (
  SELECT
    CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING)) AS session_id,
    TIMESTAMP_MICROS(event_timestamp) AS event_ts,
    event_name,
    page_location,

    source,
    medium,
    campaign,

    device_category,
    device_language,
    operating_system,

    revenue_usd,
    transaction_id
  FROM base
  WHERE ga_session_id IS NOT NULL
)
SELECT
  session_id,

  MIN(IF(event_name='session_start', event_ts, NULL)) AS session_start_ts,
  ANY_VALUE(IF(event_name='session_start', page_location, NULL)) AS landing_page,

  ANY_VALUE(source) AS source,
  ANY_VALUE(medium) AS medium,
  ANY_VALUE(campaign) AS campaign,

  ANY_VALUE(device_category) AS device_category,
  ANY_VALUE(device_language) AS device_language,
  ANY_VALUE(operating_system) AS operating_system,

  -- Funnel flags
  MAX(IF(event_name='session_start',1,0)) AS f_session_start,
  MAX(IF(event_name='view_item',1,0)) AS f_view_item,
  MAX(IF(event_name='add_to_cart',1,0)) AS f_add_to_cart,
  MAX(IF(event_name='begin_checkout',1,0)) AS f_begin_checkout,
  MAX(IF(event_name='add_shipping_info',1,0)) AS f_add_shipping_info,
  MAX(IF(event_name='add_payment_info',1,0)) AS f_add_payment_info,
  MAX(IF(event_name='purchase',1,0)) AS f_purchase,

  COUNT(DISTINCT IF(event_name='purchase', transaction_id, NULL)) AS orders,
  SUM(IF(event_name='purchase', revenue_usd, 0)) AS sales_usd
FROM session_keyed
GROUP BY session_id;
