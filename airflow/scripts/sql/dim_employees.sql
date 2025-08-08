-- Active: 1753915504673@@127.0.0.1@5434@bike_sales_dw
-- Dim_employees
CREATE OR REPLACE silver.dim_employees TABLE AS
SELECT
	e.employee_id,
	e.address_id,
	e.first_name,
	e.last_name,
	e.full_name,
	e.name_initials,
	e.login_name,
	e.email_address,
	e.gender
FROM
	bronze.employees AS e;
