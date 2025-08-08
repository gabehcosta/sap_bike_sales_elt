import pandas as pd

def transform(raw_data) -> pd.DataFrame:
    df = pd.read_csv(raw_data)

    df.columns = df.columns.str.lower()

    cols_rename = {'postalcode': 'postal_code', 
                    'addresstype': 'address_type', 
                    'validity_startdate': 'dt_start_validity', 
                    'validity_enddate': 'dt_end_validity'
                    }

    df.rename(columns=cols_rename, inplace=True)

    df['city'] = df['city'].apply(
            lambda x: x.encode('latin1').decode('utf-8') if isinstance(x, str) else x
        )

    df['duplicated_address_id'] = df.duplicated(subset='address_id', keep=False)
    duplicated_rows = df[df['duplicated_address_id'] == True]

    if duplicated_rows.shape[0] > 0:
        df.drop_duplicates(subset='address_id', inplace=True)
        df.reset_index(drop=True, inplace=True)


    df.drop(columns=['duplicated_address_id', 
                    'dt_start_validity', 
                    'address_type', 
                    'dt_end_validity'], inplace=True)

    str_cols = ['city', 
                'postal_code', 
                'street', 
                'country', 
                'region']

    for col in str_cols:
        df[col] = df[col].str.strip()

    df['building'] = df['building'].fillna(0).astype(int)

    countries_dict = {
        'US': 'United States of America',
        'CA': 'Canada',
        'DE': 'Germany',
        'GB': 'Great Britain',
        'AU': 'Australia',
        'IN': 'India',
        'DU': 'United Arab Emirates',
        'FR': 'France'
    }

    df['country'] = df['country'].replace(countries_dict)

    regions_dict = {
        'AMER': 'Americas',
        'EMEA': 'Europe, Middle East and Africa',
        'APJ': 'Asia Pacific and Japan'
    }

    df['region'] = df['region'].replace(regions_dict)

    return df