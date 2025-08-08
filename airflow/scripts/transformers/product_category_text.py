import pandas as pd

def transform(raw_data) -> pd.DataFrame:
    df = pd.read_csv(raw_data)

    df.columns = df.columns.str.lower()

    cols_rename = {
        'prodcategoryid': 'product_category_id',
        'short_descr': 'category_short_description',
        'medium_descr': 'medium_description',
        'long_descr': 'long_description'
    }

    df.rename(columns=cols_rename, inplace=True)

    df['duplicated_product_category_id'] = df.duplicated(subset='product_category_id', keep=False)
    duplicated_rows = df[df['duplicated_product_category_id'] == True]

    if duplicated_rows.shape[0] > 0:
        df.drop_duplicates(subset='product_category_id', inplace=True)
        df.reset_index(drop=True, inplace=True)


    df.drop(columns=['duplicated_product_category_id',
                    'medium_description',
                    'language',
                    'long_description'], inplace=True)

    cols = ['product_category_id',
            'category_short_description']

    for col in cols:
        df[col] = df[col].str.strip()

    return df