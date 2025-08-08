import pandas as pd

def transform(raw_data) -> pd.DataFrame:
    df = pd.read_csv(raw_data)

    df.columns = df.columns.str.lower()

    cols_rename = {
        'productid': 'product_id',
        'typecode': 'type_code',
        'prodcategoryid': 'product_category_id',
        'createdby': 'created_by_id',
        'createdat': 'dt_created_at',
        'changedby': 'changed_by_id',
        'changedat': 'dt_changed_at',
        'supplier_partnerid': 'supplier_partner_id',
        'taxtariffcode': 'tax_tariff_code',
        'quantityunit': 'qty_unit',
        'weightmeasure': 'weight_measure',
        'weightunit': 'unit',
        'dimensionunit': 'dimension_unit',
        'price': 'unit_price',
        'productpicurl': 'product_pi_curl'
    }

    df.rename(columns=cols_rename, inplace=True)

    df['duplicated_product_id'] = df.duplicated(subset='product_id', keep=False)
    duplicated_rows = df[df['duplicated_product_id'] == True]

    if duplicated_rows.shape[0] > 0:
        df.drop_duplicates(subset='product_id', inplace=True)
        df.reset_index(drop=True, inplace=True)

    str_cols = [
        'product_id',
        'type_code',
        'product_category_id',
        'qty_unit',
        'unit',
        'currency'
    ]

    for col in str_cols:
        df[col] = df[col].str.strip()

    dt_cols = [
        'dt_created_at',
        'dt_changed_at'
    ]

    for col in dt_cols:
        df[col] = pd.to_datetime(df[col].astype(str), format='%Y%m%d')

    df['unit_price'] = df['unit_price'].astype(float).round(2)

    df.drop(columns=['duplicated_product_id',
                    'width',
                    'depth',
                    'height',
                    'dimension_unit',
                    'qty_unit',
                    'tax_tariff_code',
                    'type_code',
                    'product_pi_curl'], inplace=True)
    
    return df