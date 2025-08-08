-- Active: 1753915504673@@127.0.0.1@5434@bike_sales_dw
-- Dim_partners
CREATE OR REPLACE silver.dim_partners TABLE AS
SELECT
	bp.partner_id,
	bp.address_id,
	bp.company_name,
	bp.email_address,
	bp.currency
FROM
	bronze.business_partners AS bp;
