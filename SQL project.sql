-- Dataset: https://support.google.com/analytics/answer/3437719?hl=en



-- Q1: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month

select 
    format_date("%Y%m", parse_date("%Y%m%d", _table_suffix)) as month,
    sum(totals.visits) as visits,
    sum(totals.pageviews) as pageviews,
    sum(totals.transactions) as transactions,
    sum(totals.totalTransactionRevenue)/power(10,6) as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
where _table_suffix between '20170101' and '20170331'
group by month
order by month;

-- Q2: Bounce rate per traffic source in July 2017

select 
    trafficSource.source,
    sum(totals.visits) as total_visits,
    count(totals.bounces) as total_no_of_bounces,
    count(totals.bounces)/sum(totals.visits)*100 as bounce_rate  
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`   
group by trafficSource.source
order by total_visits DESC;


-- Q3: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.

with p1 as (
    select 
        format_date("%Y%m", parse_date("%Y%m%d", _table_suffix)) as month,
        sum(totals.pageviews) as pageviews_purchasers,
        count(distinct fullVisitorId) as total_purchasers
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
    where totals.transactions >= 1
        and _table_suffix between '20170601' and '20170731'
    group by month
),

p2 as(
  select 
        format_date("%Y%m", parse_date("%Y%m%d", _table_suffix)) as month,
        sum(totals.pageviews) as pageviews_non_purchasers,
        count(distinct fullVisitorId) as total_non_purchasers
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
    where totals.transactions is null
        and _table_suffix between '20170601' and '20170731'  
    group by month
)
select 
    p1.month,
    p1.pageviews_purchasers/p1.total_purchasers as avg_pageviews_purchase,
    p2.pageviews_non_purchasers/p2.total_non_purchasers as avg_pageviews_non_purchase
from p1
inner join p2 on p1.month = p2.month
order by month;


-- Q4: Average number of transactions per user that made a purchase in 2017

select 
    format_date("%Y%m", parse_date("%Y%m%d", _table_suffix)) as Month,
    sum(totals.transactions)/count(distinct fullVisitorId) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
where _table_suffix like '2017%' and totals.transactions >= 1
group by month
order by Month;


-- Q5: Average amount of money spent per session. Only include purchaser data in July 2017

select 
    case when _table_suffix like '201707%' then '201707'
            end as Month,
    sum(totals.totalTransactionRevenue)/count(totals.visits) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
where _table_suffix like '201707%' 
    and totals.transactions is not null
group by month;

-- Q6: Calculate cohort map from pageview to addtocart to purchase in Jan, Feb and March 2017.

with product_view as (
    select 
        format_date("%Y%m", parse_date("%Y%m%d", _table_suffix)) as month,
        count(v2ProductName) as num_product_view
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        unnest(hits) as hits,
        unnest(hits.product) as product
    where hits.eCommerceAction.action_type = '2'
        and _table_suffix between '20170101' and '20170331'
    group by month
    ),

add_to_cart as (
  select 
        format_date("%Y%m", parse_date("%Y%m%d", _table_suffix)) as month,
        count(v2ProductName) as num_addtocart
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        unnest(hits) as hits,
        unnest(hits.product) as product
    where hits.eCommerceAction.action_type = '3'
        and _table_suffix between '20170101' and '20170331'
    group by month  
    ),

purchase as (
  select 
        format_date("%Y%m", parse_date("%Y%m%d", _table_suffix)) as month,
        count(v2ProductName) as num_purchase
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        unnest(hits) as hits,
        unnest(hits.product) as product
    where hits.eCommerceAction.action_type = '6'
        and _table_suffix between '20170101' and '20170331'
    group by month  
    )

select product_view.month,
    product_view.num_product_view,
    add_to_cart.num_addtocart,
    purchase.num_purchase,
    add_to_cart.num_addtocart/product_view.num_product_view*100 as add_to_cart_rate,
    purchase.num_purchase/product_view.num_product_view*100 as purchase_rate
from product_view
    left join add_to_cart on product_view.month = add_to_cart.month
    left join purchase on product_view.month = purchase.month
order by month;

