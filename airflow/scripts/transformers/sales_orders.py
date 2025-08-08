import pandas as pd

def transform(raw_data) -> pd.DataFrame:
    df = pd.read_csv(raw_data)

    df.columns = df.columns.str.lower()

    cols_rename = {
        'salesorderid': 'sales_order_id',
        'createdby': 'created_by_id',
        'createdat': 'dt_created_at',
        'changedby': 'changed_by_id',
        'changedat': 'dt_changed_at',
        'fiscvariant': 'fiscal_variant',
        'fiscalyearperiod': 'fiscal_year_period',
        'noteid': 'note_id',
        'partnerid': 'partner_id',
        'salesorg': 'sales_org',
        'grossamount': 'gross_price',
        'netamount': 'net_price',
        'taxamount': 'tax_price',
        'lifecyclestatus': 'life_cycle_status',
        'billingstatus': 'billing_status',
        'deliverystatus': 'delivery_status'
    }

    df.rename(columns=cols_rename, inplace=True)

    df['duplicated_sales_order_id'] = df.duplicated(subset='sales_order_id', keep=False)
    duplicated_rows = df[df['duplicated_sales_order_id'] == True]

    if duplicated_rows.shape[0] > 0:
        df.drop_duplicates(subset='sales_order_id', inplace=True)
        df.reset_index(drop=True, inplace=True)

    dt_cols = [
        'dt_created_at',
        'dt_changed_at'
    ]

    for col in dt_cols:
        df[col] = pd.to_datetime(df[col].astype(str), format='%Y%m%d')

    df['fiscal_year'] = df['fiscal_year_period'].astype(str).str[:4].astype(int)
    df['fiscal_month'] = df['fiscal_year_period'].astype(str).str[-2:].astype(int)

    str_cols = [
        'fiscal_variant',
        'sales_org',
        'currency',
        'life_cycle_status',
        'billing_status',
        'delivery_status'
    ]

    for col in str_cols:
        df[col] = df[col].str.strip()

    regions_dict = {
        'AMER': 'Americas',
        'EMEA': 'Europe, Middle East and Africa',
        'APJ': 'Asia Pacific and Japan'
    }

    df['sales_org'] = df['sales_org'].replace(regions_dict)

    life_cycle_status_dict = {
        'C': 'Completed',
        'I': 'In Progress',
        'X': 'Canceled'
    }

    df['life_cycle_status'] = df['life_cycle_status'].replace(life_cycle_status_dict)

    billing_status_dict = {
        'C': 'Billed',
        'I': 'Awaiting Billing',
        'X': 'Canceled'
    }

    df['billing_status'] = df['billing_status'].replace(billing_status_dict)

    delivery_status_dict = {
        'C': 'Delivered',
        'I': 'In Transit',
        'X': 'Canceled'
    }

    df['delivery_status'] = df['delivery_status'].replace(delivery_status_dict)

    df['gross_price'] = df['gross_price'].astype(float)

    price_cols = [
        'gross_price',
        'net_price',
        'tax_price'
    ]

    for col in price_cols:
        df[col] = df[col].astype(float)
        df[col] = df[col].round(2)

    df.drop(columns=['duplicated_sales_order_id',
                     'fiscal_year_period',
                     'fiscal_variant',
                     'note_id'], inplace=True)
    
    return df