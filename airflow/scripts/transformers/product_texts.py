import pandas as pd

def transform(raw_data) -> pd.DataFrame:
    df = pd.read_csv(raw_data)

    df.columns = df.columns.str.lower()

    cols_rename = {
        'productid': 'product_id',
        'short_descr': 'short_description',
        'medium_descr': 'medium_description',
        'long_descr': 'long_description'
    }

    df.rename(columns=cols_rename, inplace=True)

    df['duplicated_product_id'] = df.duplicated(subset='product_id', keep=False)
    duplicated_rows = df[df['duplicated_product_id'] == True]

    if duplicated_rows.shape[0] > 0:
        df.drop_duplicates(subset='product_id', inplace=True)
        df.reset_index(drop=True, inplace=True)

    df['product_description'] = df['medium_description'].fillna(df['short_description'])

    df.drop(columns=['duplicated_product_id',
                    'language',
                    'short_description',
                    'medium_description',
                    'long_description'], inplace=True)

    cols = ['product_id', 'product_description']

    for col in cols:
        df[col] = df[col].str.strip()

    return df