import pandas as pd

def transform(raw_data) -> pd.DataFrame:
    df = pd.read_csv(raw_data)

    df.columns = df.columns.str.lower()

    cols_rename = {
        'salesorderid': 'sales_order_id',
        'salesorderitem': 'sales_order_item',
        'productid': 'product_id',
        'noteid': 'note_id',
        'grossamount': 'gross_price',
        'netamount': 'net_price',
        'taxamount': 'tax_price',
        'itematpstatus': 'item_at_p_status',
        'quantity': 'qty',
        'quantityunit': 'qty_unit',
        'deliverydate': 'dt_delivery'
    }

    df.rename(columns=cols_rename, inplace=True)

    df['id_sale_line'] = df['sales_order_id'].astype(str) + '-' + df['sales_order_item'].astype(str)

    df['duplicated_id_sale_line'] = df.duplicated(subset='id_sale_line', keep=False)
    duplicated_rows = df[df['duplicated_id_sale_line'] == True]

    if duplicated_rows.shape[0] > 0:
        df.drop_duplicates(subset='id_sale_line', inplace=True)
        df.reset_index(drop=True, inplace=True)

    df.drop(columns=['duplicated_id_sale_line',
                    'note_id',
                    'item_at_p_status',
                    'qty_unit',
                    'opitempos'], inplace=True)

    str_cols = [
        'product_id',
        'currency',
        'id_sale_line'
    ]

    for col in str_cols:
        df[col] = df[col].str.strip()

    df['dt_delivery'] = df['dt_delivery'].astype(str)
    df['dt_delivery'] = df['dt_delivery'].replace('29991212', pd.NaT)

    df['dt_delivery'] = pd.to_datetime(
        df['dt_delivery'], 
        format='%Y%m%d'
    )

    df['gross_price'] = df['gross_price'].astype(float).round(2)
    df['net_price'] = df['net_price'].round(2)
    df['tax_price'] = df['tax_price'].round(2)

    df['unit_gross_price'] = (df['gross_price'] / df['qty']).round(2)
    df['unit_net_price'] = (df['net_price'] / df['qty']).round(2)
    df['unit_tax_price'] = (df['tax_price'] / df['qty']).round(2)

    return df