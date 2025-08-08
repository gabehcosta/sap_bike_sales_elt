-- Active: 1753915504673@@127.0.0.1@5434@bike_sales_dw
-- Dim_addresses
CREATE OR REPLACE silver.dim_addresses TABLE AS
SELECT
	ad.address_id,
	ad.street,
	ad.building,
	ad.postal_code,
	ad.city,
	ad.country,
	ad.region,
	ad.latitude,
	ad.longitude
FROM
	bronze.addresses as ad;
