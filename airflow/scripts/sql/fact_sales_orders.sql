-- Active: 1753915504673@@127.0.0.1@5434@bike_sales_dw
-- Fact_sales_orders
CREATE OR REPLACE TABLE silver.fact_sales_orders AS
SELECT
	soi.id_sale_line,
	soi.sales_order_id,
	soi.sales_order_item,
	UPPER(so.life_cycle_status) AS life_cycle_status,
	UPPER(so.billing_status) AS billing_status,
	UPPER(so.delivery_status) AS delivery_status,
	soi.product_id,
	so.partner_id,
	so.created_by_id,
	DATE(so.dt_created_at) AS dt_created_at,
	DATE(soi.dt_delivery) AS dt_delivery,
	so.fiscal_month,
	so.fiscal_year,
	soi.qty,
	ROUND(soi.unit_gross_price, 2) AS unit_gross_price,
	ROUND(soi.gross_price, 2) AS total_gross_price,
	ROUND(soi.unit_tax_price, 2) AS unit_tax_price,
	ROUND(soi.tax_price, 2) AS total_tax_price,
	ROUND(soi.unit_net_price, 2) AS unit_net_price,
	ROUND(soi.net_price, 2) AS total_net_price,
	UPPER(soi.currency) AS currency
FROM
	bronze.sales_order_items AS soi
INNER JOIN
	bronze.sales_orders AS so
	ON so.sales_order_id = soi.sales_order_id
WHERE 
	soi.sales_order_id IS NOT NULL
ORDER BY
	dt_created_at ASC,
	id_sale_line ASC;
