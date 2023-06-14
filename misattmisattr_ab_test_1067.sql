-- test 1067

WITH test_info AS
(
SELECT
1067 AS ab_test_id,
2373 AS control_group,
2374 AS test_group
),

uncapped_rev_per_auction AS
(SELECT
COALESCE(install__ad_click__impression__auction_id,
reeng_click__impression__auction_id,
attribution_event__click__impression__auction_id) AS auction_id
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(COALESCE(attribution_event__click__impression__at, reeng_click__impression__at, install__ad_click__impression__at)/1000, 'UTC'))),1,19),'Z') AS impression_at
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(COALESCE(attribution_event__click__at, reeng_click__at, install__ad_click__at)/1000, 'UTC'))),1,19),'Z') AS click_at
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(install__at/1000, 'UTC'))),1,19),'Z') AS install_at
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
, "group".id AS ab_test_group_id
, COALESCE(install__ad_click__impression__bid__customer_id,
reeng_click__impression__bid__customer_id,
attribution_event__click__impression__bid__customer_id) AS customer_id
, COALESCE(install__ad_click__impression__bid__app_id,
reeng_click__impression__bid__app_id,
attribution_event__click__impression__bid__app_id) AS dest_app_id
, CAST(COALESCE(install__ad_click__impression__bid__bid_request__non_personalized,
reeng_click__impression__bid__bid_request__non_personalized,
attribution_event__click__impression__bid__bid_request__non_personalized) AS varchar) AS non_personalized
, CAST(is_viewthrough AS varchar) AS is_viewthrough
, COALESCE(attribution_event__click__impression__bid__bid_request__device__family, reeng_click__impression__bid__bid_request__device__family, install__ad_click__impression__bid__bid_request__device__family) AS device_family
, CASE WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'video' THEN 'VAST'
WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'native' THEN 'native'
WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) in ('320x50', '728x90') THEN 'banner'
WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = '300x250' THEN 'mrec'
ELSE 'html-interstitial' END AS  ad_format
, COALESCE(attribution_event__click__impression__bid__creative__type, reeng_click__impression__bid__creative__type, install__ad_click__impression__bid__creative__type) AS creative_type
, IF(install__tracker_params__idfa NOT IN ('', '00000000-0000-0000-0000-000000000000') AND install__tracker_params__idfa = install__ad_click__impression__bid__bid_request__device__platform_specific_id,'yes','no') AS idfa_match
, IF(install__tracker_params__idfa in ('', '00000000-0000-0000-0000-000000000000') OR install__ad_click__impression__bid__bid_request__device__platform_specific_id IN ('', '00000000-0000-0000-0000-000000000000'),'yes','no') AS idfa_missing
, install__tracker_params__match_type AS match_type
, install__ad_click__impression__bid__bid_request__device__model_data__name AS bidrequest_normalized_device_model
, IF(install__ad_click__impression__bid__bid_request__device__model_data__name = pod_device_model,'yes','no') AS normalized_device_model_match
, IF(install__ad_click__impression__bid__bid_request__device__model_data__name LIKE '%Unknown' OR pod_device_model LIKE '%Unknown' OR '' IN (pod_device_model, install__ad_click__impression__bid__bid_request__device__model_data__name) ,'yes','no') AS unknown_device_model
, sum(0) AS impressions
, sum(0) AS spend_micros
, sum(0) AS revenue_micros
, sum(0) AS clicks
, sum(0) AS installs
, sum(0) AS unreported_installs
, sum(IF(for_reporting AND custom_event_id = COALESCE(install__ad_click__impression__bid__campaign_target_event_id, reeng_click__impression__bid__campaign_target_event_id),1,0)) AS target_events
, sum(IF(NOT for_reporting AND custom_event_id = COALESCE(install__ad_click__impression__bid__campaign_target_event_id, reeng_click__impression__bid__campaign_target_event_id),1,0)) AS unreported_target_events
, sum(IF(for_reporting AND customer_revenue_micros > -100000000000 AND customer_revenue_micros < 100000000000,customer_revenue_micros)) AS customer_revenue_micros
, sum(IF(NOT for_reporting AND customer_revenue_micros > -100000000000 AND customer_revenue_micros < 100000000000,customer_revenue_micros)) AS unreported_customer_revenue_micros
FROM rtb.matched_app_events ae
CROSS JOIN UNNEST(COALESCE(
install__ad_click__impression__bid__bid_request__ab_test_assignments,
reeng_click__impression__bid__bid_request__ab_test_assignments)) t
WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
--dt >= '2023-06-12T01' AND dt < '2023-06-20T03'
AND is_uncredited <> true
and t.id = (SELECT ab_test_id FROM test_info)
AND COALESCE(install__ad_click__impression__bid__campaign_tracker_type,
reeng_click__impression__bid__campaign_tracker_type,
attribution_event__click__impression__bid__campaign_tracker_type) != 'SKAN'
AND COALESCE(attribution_event__click__impression__bid__ad_group_type, reeng_click__impression__bid__ad_group_type, install__ad_click__impression__bid__ad_group_type) = 'user-acquisition'
AND COALESCE(attribution_event__click__impression__bid__app_platform, reeng_click__impression__bid__app_platform, install__ad_click__impression__bid__app_platform) = 'IOS'
AND IF(skan_params__version IS NULL OR skan_params__version = ''
, COALESCE(attribution_event__click__impression__bid__sk_ad_network_response__version, reeng_click__impression__bid__sk_ad_network_response__version, install__ad_click__impression__bid__sk_ad_network_response__version), skan_params__version) IS NOT NULL -- skan_compatible
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)
,
funnel AS
(
SELECT
CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(at/1000, 'UTC'))),1,19),'Z') AS impression_at
, null AS click_at
, null AS install_at
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(at/1000, 'UTC'))),1,19),'Z') AS at
, "group".id AS ab_test_group_id
, bid__customer_id AS customer_id
, bid__app_id AS dest_app_id
, CAST(bid__bid_request__non_personalized AS varchar) AS non_personalized
, 'N/A' AS is_viewthrough
, bid__bid_request__device__family AS device_family
, CASE WHEN bid__creative__ad_format = 'video' THEN 'VAST'
WHEN bid__creative__ad_format = 'native' THEN 'native'
WHEN bid__creative__ad_format in ('320x50', '728x90') THEN 'banner'
WHEN bid__creative__ad_format = '300x250' THEN 'mrec'
ELSE 'html-interstitial' END AS  ad_format
, bid__creative__type AS creative_type
, 'N/A' AS idfa_match
, 'N/A' AS idfa_missing
, 'N/A' AS match_type
, 'N/A' AS bidrequest_normalized_device_model
, 'N/A' AS normalized_device_model_match
, 'N/A' AS unknown_device_model
, sum(1) AS impressions
, sum(spend_micros) AS spend_micros
, sum(revenue_micros) AS revenue_micros
, sum(0) AS clicks
, sum(0) AS installs
, sum(0) AS unreported_installs
, sum(0) AS target_events
, sum(0) AS unreported_target_events
, sum(0) AS customer_revenue_micros
, sum(0) AS unreported_customer_revenue_micros
, sum(0) AS capped_customer_revenue_micros
, sum(0) AS squared_capped_customer_revenue
FROM rtb.impressions_with_bids i
CROSS JOIN UNNEST(bid__bid_request__ab_test_assignments) t
WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours = 1) }}'
--dt >= '2023-06-12T01' AND dt < '2023-06-20T03'
AND t.id = (SELECT ab_test_id FROM test_info)
AND bid__campaign_tracker_type != 'SKAN'
AND bid__app_platform = 'IOS'
AND bid__ad_group_type = 'user-acquisition'
AND bid__sk_ad_network_response__version IS NOT NULL -- skan_compatible
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

UNION ALL

SELECT
CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(impression__at/1000, 'UTC'))),1,19),'Z') AS impression_at
, CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS click_at
, NULL AS install_at
, CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS at
, "group".id AS ab_test_group_id
, impression__bid__customer_id as customer_id
, impression__bid__app_id as dest_app_id
, CAST(impression__bid__bid_request__non_personalized AS varchar) AS is_nonpersonalized
, 'N/A' AS is_viewthrough
, impression__bid__bid_request__device__family AS device_family
, CASE WHEN impression__bid__creative__ad_format = 'video' THEN 'VAST'
WHEN impression__bid__creative__ad_format = 'native' THEN 'native'
WHEN impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
WHEN  impression__bid__creative__ad_format = '300x250' THEN 'mrec'
ELSE 'html-interstitial' END AS ad_format
, impression__bid__creative__type AS creative_type
, 'N/A' AS idfa_match
, 'N/A' AS idfa_missing
, 'N/A' AS match_type
, 'N/A' AS bidrequest_normalized_device_model
, 'N/A' AS normalized_device_model_match
, 'N/A' AS unknown_device_model
, sum(0) AS impressions
, sum(0) AS spend_micros
, sum(0) AS revenue_micros
, sum(1) AS clicks
, sum(0) AS installs
, sum(0) AS unreported_installs
, sum(0) AS target_events
, sum(0) AS unreported_target_events
, sum(0) AS customer_revenue_micros
, sum(0) AS unreported_customer_revenue_micros
, sum(0) AS capped_customer_revenue_micros
, sum(0) AS squared_capped_customer_revenue
FROM rtb.ad_clicks ac
CROSS JOIN UNNEST(impression__bid__bid_request__ab_test_assignments) t
WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours = 1) }}'
--dt >= '2023-06-12T01' AND dt < '2023-06-20T03'
AND has_prior_click = FALSE
AND t.id = (SELECT ab_test_id FROM test_info)
AND impression__bid__campaign_tracker_type != 'SKAN'
AND impression__bid__app_platform = 'IOS'
AND impression__bid__ad_group_type = 'user-acquisition'
AND impression__bid__sk_ad_network_response__version IS NOT NULL -- skan_compatible
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

UNION ALL

SELECT
CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(impression__at/1000, 'UTC'))),1,19),'Z') AS impression_at
, CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS click_at
, NULL AS install_at
, CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS at
, "group".id AS ab_test_group_id
, impression__bid__customer_id as customer_id
, impression__bid__app_id as dest_app_id
, CAST(impression__bid__bid_request__non_personalized AS varchar) AS is_nonpersonalized
, 'N/A' AS is_viewthrough
, impression__bid__bid_request__device__family AS device_family
, CASE WHEN impression__bid__creative__ad_format = 'video' THEN 'VAST'
WHEN impression__bid__creative__ad_format = 'native' THEN 'native'
WHEN impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
WHEN  impression__bid__creative__ad_format = '300x250' THEN 'mrec'
ELSE 'html-interstitial' END AS ad_format
, impression__bid__creative__type AS creative_type
, 'N/A' AS idfa_match
, 'N/A' AS idfa_missing
, 'N/A' AS match_type
, 'N/A' AS bidrequest_normalized_device_model
, 'N/A' AS normalized_device_model_match
, 'N/A' AS unknown_device_model
, sum(0) AS impressions
, sum(0) AS spend_micros
, sum(0) AS revenue_micros
, sum(1) AS clicks
, sum(0) AS installs
, sum(0) AS unreported_installs
, sum(0) AS target_events
, sum(0) AS unreported_target_events
, sum(0) AS customer_revenue_micros
, sum(0) AS unreported_customer_revenue_micros
, sum(0) AS capped_customer_revenue_micros
, sum(0) AS squared_capped_customer_revenue
FROM rtb.view_clicks vc
CROSS JOIN UNNEST(impression__bid__bid_request__ab_test_assignments) t
WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours = 1) }}'
--dt >= '2023-06-12T01' AND dt < '2023-06-20T03'
AND has_prior_click = FALSE
AND t.id = (SELECT ab_test_id FROM test_info)
AND impression__bid__campaign_tracker_type != 'SKAN'
AND impression__bid__app_platform = 'IOS'
AND impression__bid__ad_group_type = 'user-acquisition'
AND impression__bid__sk_ad_network_response__version IS NOT NULL -- skan_compatible
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

UNION ALL

SELECT
CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(ad_click__impression__at/1000, 'UTC'))),1,19),'Z') AS impression_at
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(ad_click__at/1000, 'UTC'))),1,19),'Z') AS click_at
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS install_at
, CONCAT(SUBSTR(to_iso8601(date_trunc('hour', FROM_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
, "group".id AS ab_test_group_id
, ad_click__impression__bid__customer_id AS customer_id
, ad_click__impression__bid__app_id AS dest_app_id
, CAST(ad_click__impression__bid__bid_request__non_personalized AS varchar) AS non_personalized
, CAST(is_viewthrough AS VARCHAR) AS is_viewthrough
, ad_click__impression__bid__bid_request__device__family AS device_family
, CASE WHEN ad_click__impression__bid__creative__ad_format = 'video' THEN 'VAST'
WHEN ad_click__impression__bid__creative__ad_format = 'native' THEN 'native'
WHEN ad_click__impression__bid__creative__ad_format in ('320x50', '728x90') THEN 'banner'
WHEN ad_click__impression__bid__creative__ad_format = '300x250' THEN 'mrec'
ELSE 'html-interstitial' end AS ad_format
, ad_click__impression__bid__creative__type AS creative_type
, IF(tracker_params__idfa not in ('', '00000000-0000-0000-0000-000000000000') and tracker_params__idfa = ad_click__impression__bid__bid_request__device__platform_specific_id,'yes','no') AS idfa_match
, IF(tracker_params__idfa in ('', '00000000-0000-0000-0000-000000000000') or ad_click__impression__bid__bid_request__device__platform_specific_id in ('', '00000000-0000-0000-0000-000000000000'),'yes','no') AS idfa_missing
, tracker_params__match_type AS match_type
, ad_click__impression__bid__bid_request__device__model_data__name AS bidrequest_normalized_device_model
, IF(ad_click__impression__bid__bid_request__device__model_data__name = pod_device_model ,'yes','no') AS normalized_device_model_match
, IF(ad_click__impression__bid__bid_request__device__model_data__name LIKE '%Unknown' OR pod_device_model LIKE '%Unknown' OR '' IN (pod_device_model, ad_click__impression__bid__bid_request__device__model_data__name) ,'yes','no') AS unknown_device_model
, sum(0) AS impressions
, sum(0) AS spend_micros
, sum(0) AS revenue_micros
, sum(0) AS clicks
, sum(IF(for_reporting, 1, 0)) AS installs
, sum(IF(NOT for_reporting, 1, 0)) AS unreported_installs
, sum(0) AS target_events
, sum(0) AS unreported_target_events
, sum(0) AS customer_revenue_micros
, sum(0) AS unreported_customer_revenue_micros
, sum(0) AS capped_customer_revenue_micros
, sum(0) AS squared_capped_customer_revenue
FROM rtb.matched_installs mi
CROSS JOIN UNNEST(ad_click__impression__bid__bid_request__ab_test_assignments) t
WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours = 1) }}'
--dt >= '2023-06-12T01' AND dt < '2023-06-20T03'
AND is_uncredited <> true
AND t.id = (SELECT ab_test_id FROM test_info)
AND ad_click__impression__bid__campaign_tracker_type != 'SKAN'
AND ad_click__impression__bid__ad_group_type = 'user-acquisition'
AND ad_click__impression__bid__app_platform = 'IOS'
AND IF(skan_params__version IS NULL OR skan_params__version = ''
, ad_click__impression__bid__sk_ad_network_response__version, skan_params__version) IS NOT NULL -- skan_compatible
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

UNION ALL

SELECT
impression_at
, click_at
, install_at
, at
, ab_test_group_id
, customer_id
, dest_app_id
, non_personalized
, is_viewthrough
, device_family
, ad_format
, creative_type
, idfa_match
, idfa_missing
, match_type
, bidrequest_normalized_device_model
, normalized_device_model_match
, unknown_device_model
, sum(0) AS impressions
, sum(0) AS spend_micros
, sum(revenue_micros) AS revenue_micros
, sum(0) AS clicks
, sum(0) AS installs
, sum(0) AS unreported_installs
, sum(0) AS target_events
, sum(0) AS unreported_target_events
, sum(0) AS customer_revenue_micros
, sum(0) AS unreported_customer_revenue_micros
, sum(least(customer_revenue_micros,500000000)) AS capped_customer_revenue_micros
, sum(power(least(CAST(customer_revenue_micros AS double)/1000000,500),2)) AS squared_capped_customer_revenue
FROM uncapped_rev_per_auction u
--dt >= '2023-03-30T00' --AND dt < '2023-03-24T02'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)
SELECT
impression_at
, click_at
, install_at
, ab_test_group_id
, atg.name AS test_group_name
, a.customer_id
, a.dest_app_id
, non_personalized
, is_viewthrough
, ad_format
, creative_type
, device_family
, b.company AS customer_name
, c.display_name AS dest_app_name
, c.salesforce_account_level AS account_level
, sum(impressions) AS impressions
, sum(spend_micros) AS spend_micros
, sum(revenue_micros) AS revenue_micros
, sum(clicks) AS clicks
, sum(installs) AS installs
, SUM(unreported_installs) AS unreported_installs
, sum(target_events) AS target_events
, sum(unreported_target_events) AS unreported_target_events
, sum(customer_revenue_micros) AS customer_revenue_micros
, SUM(unreported_customer_revenue_micros) AS unreported_customer_revenue_micros
, sum(capped_customer_revenue_micros) AS capped_customer_revenue_micros
, sum(squared_capped_customer_revenue) AS squared_capped_customer_revenue
, sum(CASE WHEN ((match_type = 'DETERMINISTIC' OR idfa_match = 'yes') OR (normalized_device_model_match = 'yes' AND unknown_device_model = 'no')) THEN installs END) AS strongly_matched_installs -- installs we are confident were matched correctly
, sum(CASE WHEN ((match_type = 'PROBABILISTIC' OR idfa_missing = 'yes') and normalized_device_model_match <> 'yes' and unknown_device_model <> 'yes') THEN installs END) AS misattributed_installs -- installs that we are confident were mismatched (via p-matching) based on device model
, sum(CASE WHEN ((match_type = 'PROBABILISTIC' OR idfa_missing = 'yes') and unknown_device_model = 'yes') THEN installs END) AS possibly_misattributed_installs -- installs that are not a strong match, but we can't know for sure they were matched incorrectly because missing device model data
FROM funnel a
LEFT JOIN pinpoint.public.customers b
ON a.customer_id = b.id
LEFT JOIN pinpoint.public.apps c
ON a.dest_app_id = c.id
and a.customer_id = c.customer_id
LEFT JOIN pinpoint.public.ab_test_groups atg
ON ab_test_group_id = atg.id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
