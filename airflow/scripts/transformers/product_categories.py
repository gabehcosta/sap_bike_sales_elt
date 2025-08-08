import pandas as pd

def transform(raw_data) -> pd.DataFrame:
    df = pd.read_csv(raw_data)

    df.columns = df.columns.str.lower()

    cols_rename = {
        'prodcategoryid': 'product_category_id',
        'createdby': 'created_by_id',
        'createdat': 'dt_created_at'
    }

    df.rename(columns=cols_rename, inplace=True)

    df['duplicated_product_category_id'] = df.duplicated(subset='product_category_id', keep=False)
    duplicated_rows = df[df['duplicated_product_category_id'] == True]

    if duplicated_rows.shape[0] > 0:
        df.drop_duplicates(subset='product_category_id', inplace=True)
        df.reset_index(drop=True, inplace=True)

    df['dt_created_at'] = pd.to_datetime(df['dt_created_at'].astype(str), format='%Y%m%d')

    df['product_category_id'] = df['product_category_id'].str.strip()

    df.drop(columns=['duplicated_product_category_id',], inplace=True)

    return df