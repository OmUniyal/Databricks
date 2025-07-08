-- create materialized view showing active customers

create materialized view om_gold.active_customers AS
select * from om_silver.customers_scd2 where __END_AT IS null;


-- create materialized view showing top products by sales for active customers

create materialized view om_gold.top_product_by_active_customer as
select
  c.customer_id,
  c.customer_name,
  p.product_id,
  p.product_name,
  p.product_category,
  sum(s.total_amount) as total_sales,
  count(distinct s.order_id) as order_count
from om_gold.active_customers c
join om_silver.sales_pipeline_cleaned s
  on c.customer_id = s.customer_id
join om_silver.products_cleaned p
  on s.product_id = p.product_id
group by all
order by total_sales desc
limit 3;