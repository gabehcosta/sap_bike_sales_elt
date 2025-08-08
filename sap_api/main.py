from fastapi import FastAPI, HTTPException, Query
import sqlite3

app = FastAPI()

# Absolute path to the SQLite database file inside the Docker container
DB_PATH = '/app/data/sap_api.db'

# Tables that are allowed
ALLOWED_TABLES = {
    'addresses',
    'business_partners',
    'employees',
    'product_categories',
    'product_category_text',
    'product_texts',
    'products',
    'sales_order_items',
    'sales_orders'
}

# Default order by column for each table
DEFAULT_ORDER_BY = {
    'addresses': 'ADDRESS_ID',
    'business_partners': 'PARTNERID',
    'employees': 'EMPLOYEEID',
    'product_categories': 'PRODCATEGORYID',
    'product_category_text': 'PRODCATEGORYID',
    'product_texts': 'PRODUCTID',
    'products': 'PRODUCTID',
    'sales_order_items': 'SALESORDERID',
    'sales_orders': 'SALESORDERID'
}

def select_table(table_name: str, limit: int, offset: int, order_by: str) -> list[dict]:
    """
    Executes a SELECT query with ORDER BY and pagination on a specific table.

    Args:
        table_name (str): Name of the table to query.
        limit (int): Number of records to return.
        offset (int): Number of records to skip.
        order_by (str): Column to sort the results by.

    Returns:
        list[dict]: List of records as dictionaries.

    Raises:
        HTTPException: If the query fails.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        query = f"SELECT * FROM {table_name} ORDER BY {order_by} LIMIT {limit} OFFSET {offset}"
        cur.execute(query)
        rows = cur.fetchall()
        return [dict(row) for row in rows]
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        conn.close()

def count_rows(table_name: str) -> int:
    """
    Returns the total number of rows in a table.

    Args:
        table_name (str): Table to count rows from.

    Returns:
        int: Total number of rows in the table.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        query = f"SELECT COUNT(*) FROM {table_name}"
        cur.execute(query)
        total = cur.fetchone()[0]
        return total
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error counting rows: {str(e)}")
    
    finally:
        conn.close()

@app.get("/")
def read_root():
    """
    Root endpoint that returns basic API information and available endpoints.

    Returns:
        dict: API welcome message and usage instructions.
    """
    return {
        "message": "Welcome to the Fake SAP REST API!",
        "usage": {
            "endpoint": "/{table_name}",
            "description": "Retrieve data from a specific table using pagination.",
            "query_params": {
                "page": "Page number (starting from 1)"
            },
            "records_per_page": 100,
            "allowed_tables": sorted(list(ALLOWED_TABLES))
        }
    }

@app.get("/{table_name}")
def get_table_data(
    table_name: str,
    page: int = Query(1, gt=0, description="Page number (minimum 1)")
):
    """
    Returns a paginated list of records from a specified table.

    Args:
        table_name (str): Table name (must be in ALLOWED_TABLES).
        page (int): Page number (starting at 1).

    Returns:
        dict: Object containing metadata and results.

    Raises:
        HTTPException: If the table is not allowed or the page is out of range.
    """
    if table_name not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail=f"Table '{table_name}' is not allowed.")

    limit = 100  # Fixed records per page
    total_rows = count_rows(table_name)
    total_pages = max(1, (total_rows + limit - 1) // limit)

    if page > total_pages:
        raise HTTPException(
            status_code=404,
            detail=f"Page {page} is out of range. Total pages: {total_pages}."
        )

    offset = (page - 1) * limit
    order_by = DEFAULT_ORDER_BY.get(table_name)

    if not order_by:
        raise HTTPException(status_code=500, detail=f"No default 'order_by' field defined for table '{table_name}'.")

    data = select_table(table_name, limit, offset, order_by)

    return {
        "table_name": table_name,
        "total_rows": total_rows,
        "total_pages": total_pages,
        "page": page,
        "count": len(data),
        "results": data
    }
