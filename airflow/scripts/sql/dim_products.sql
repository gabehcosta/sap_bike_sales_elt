-- Active: 1754439353175@@127.0.0.1@5434@bike_sales_dw
-- Dim_products
CREATE TABLE silver.dim_products AS
SELECT
    prd.product_id,
    prd.supplier_partner_id,
    prd_cat.category_short_description,
    prd_txt.product_description,
    prd.unit_price,
    prd.currency,
    prd.weight_measure,
    prd.unit
FROM
    bronze.products AS prd
INNER JOIN
    bronze.product_category_text AS prd_cat
    ON prd_cat.product_category_id = prd.product_category_id
INNER JOIN
    bronze.product_texts AS prd_txt
    ON prd_txt.product_id = prd.product_id;

--Procedure
CREATE OR REPLACE PROCEDURE process_dim_products()
LANGUAGE plpgsql AS $$
BEGIN
	TRUNCATE TABLE silver.dim_products;

	INSERT INTO silver.dim_products (
        product_id,
        supplier_partner_id,
        category_short_description,
        product_description,
        unit_price,
        currency,
        weight_measure,
        unit
    )
	SELECT
		prd.product_id,
		prd.supplier_partner_id,
		prd_cat.category_short_description,
		prd_txt.product_description,
		prd.unit_price,
		prd.currency,
		prd.weight_measure,
		prd.unit
	FROM
		bronze.products as prd
	INNER JOIN
		bronze.product_category_text AS prd_cat
	ON
		prd_cat.product_category_id = prd.product_category_id
	INNER JOIN
		bronze.product_texts AS prd_txt
	ON
		prd_txt.product_id = prd.product_id;
END;
$$;
