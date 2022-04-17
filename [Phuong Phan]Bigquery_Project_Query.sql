-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL

select 
    case when _table_suffix like '201701%' then '201701'
        when _table_suffix like '201702%' then '201702'
        when _table_suffix like '201703%' then '201703' 
        end as month,
    sum(totals.visits) as visits,
    sum(totals.pageviews) as pageviews,
    sum(totals.transactions) as transactions,
    sum(totals.totalTransactionRevenue)/power(10,6) as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
where _table_suffix between '20170101' and '20170331'
group by month
order by month;


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL

select 
    trafficSource.source,
    sum(totals.visits) as total_visits,
    count(totals.bounces) as total_no_of_bounces,
    count(totals.bounces)/sum(totals.visits)*100 as bounce_rate  
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`   
group by trafficSource.source
order by total_visits DESC;


-- Query 3: Revenue by traffic source by week, by month in June 2017

Q3:
select 
    case when _table_suffix like '201706%' then 'Month'
        end as time_type,
    case when _table_suffix like '201706%' then '201706'
        end as time,
    trafficSource.source,
    sum(totals.totalTransactionRevenue)/power(10,6) as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
where _table_suffix like '201706%'
group by time_type, time, source
having revenue <> 0

union all

select 
    case when _table_suffix like '201706%' then 'Week' 
        end as time_type,
    concat(extract(year from parse_date('%Y%m%d', _table_suffix)), extract(week from parse_date('%Y%m%d', _table_suffix))) as time,
    trafficSource.source,
    sum(totals.totalTransactionRevenue)/power(10,6)  as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
where _table_suffix between '20170601' and '20170630'
group by time_type, time, source
having revenue <> 0
order by revenue DESC;
Q1:
select 
    case when _table_suffix like '201701%' then '201701'
        when _table_suffix like '201702%' then '201702'
        when _table_suffix like '201703%' then '201703' 
        end as month,
    sum(totals.visits) as visits,
    sum(totals.pageviews) as pageviews,
    sum(totals.transactions) as transactions,
    sum(totals.totalTransactionRevenue)/power(10,6) as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
where _table_suffix between '20170101' and '20170331'
group by month
order by month;


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

with p1 as (
    select 
        case when _table_suffix like '201706%' then '201706'
             when _table_suffix like '201707%' then '201707'
            end as month,
        sum(totals.pageviews) as pageviews_purchasers,
        count(distinct fullVisitorId) as total_purchasers
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
    where totals.transactions >= 1
        and _table_suffix between '20170601' and '20170731'
    group by month
),

p2 as(
  select 
        case when _table_suffix like '201706%' then '201706'
             when _table_suffix like '201707%' then '201707'
            end as month,
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


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

select 
    case when _table_suffix like '201707%' then '201707'
            end as Month,
    sum(totals.transactions)/count(distinct fullVisitorId) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
where _table_suffix like '201707%' and totals.transactions >= 1
group by month;


-- Query 06: Average amount of money spent per session
#standardSQL

select 
    case when _table_suffix like '201707%' then '201707'
            end as Month,
    sum(totals.totalTransactionRevenue)/count(totals.visits) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
where _table_suffix like '201707%' 
    and totals.transactions is not null
group by month;


-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL

with data2 as (
select 
    fullVisitorId as customerId,
    v2ProductName as other_purchased_products,
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) as hits,
    UNNEST(hits.product) as product
where _table_suffix like '201707%' 
    and product.productRevenue is not null 
)

select
    data2.other_purchased_products,
    count(data2.other_purchased_products) as quantity  
from data2
where data2.customerId in (
            SELECT
            distinct fullVisitorId as customerId,
        from 
            `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
            UNNEST(hits) as hits,
            UNNEST(hits.product) as product
        where _table_suffix like '201707%' 
            and product.productRevenue is not null 
            and v2ProductName = "YouTube Men's Vintage Henley"
        )
        
group by data2.other_purchased_products
order by quantity DESC;


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

with data1 as (
    select 
        concat(extract(year from parse_date('%Y%m%d', _table_suffix)), extract(month from parse_date('%Y%m%d', _table_suffix))) as month,
        count(v2ProductName) as num_product_view
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        unnest(hits) as hits,
        unnest(hits.product) as product
    where hits.eCommerceAction.action_type = '2'
        and _table_suffix between '20170101' and '20170331'
    group by month
    ),

data2 as (
  select 
        concat(extract(year from parse_date('%Y%m%d', _table_suffix)), extract(month from parse_date('%Y%m%d', _table_suffix))) as month,
        count(v2ProductName) as num_addtocart
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        unnest(hits) as hits,
        unnest(hits.product) as product
    where hits.eCommerceAction.action_type = '3'
        and _table_suffix between '20170101' and '20170331'
    group by month  
    ),

data3 as (
  select 
        concat(extract(year from parse_date('%Y%m%d', _table_suffix)), extract(month from parse_date('%Y%m%d', _table_suffix))) as month,
        count(v2ProductName) as num_purchase
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        unnest(hits) as hits,
        unnest(hits.product) as product
    where hits.eCommerceAction.action_type = '6'
        and _table_suffix between '20170101' and '20170331'
    group by month  
    )

select data1.month,
    data1.num_product_view,
    data2.num_addtocart,
    data3.num_purchase,
    data2.num_addtocart/data1.num_product_view*100 as add_to_cart_rate,
    data3.num_purchase/data1.num_product_view*100 as purchase_rate
from data1
    inner join data2 on data1.month = data2.month
    inner join data3 on data1.month = data3.month
order by month;