import pandas as pd

def transform(raw_data) -> pd.DataFrame:
    df = pd.read_csv(raw_data)

    df.columns = df.columns.str.lower()

    cols_rename = {
        'partnerid':'partner_id',
        'partnerrole':'partner_role',
        'emailaddress':'email_address',
        'phonenumber':'phone_number',
        'faxnumber':'fax_number',
        'webaddress':'web_address',
        'addressid':'address_id',
        'companyname':'company_name',
        'legalform':'legal_form',
        'createdby':'created_by_id',
        'createdat':'dt_created_at',
        'changedby':'changed_by_id',
        'changedat':'dt_changed_at'
    }

    df.rename(columns=cols_rename, inplace=True)

    df['duplicated_partner_id'] = df.duplicated(subset='partner_id', keep=False)
    duplicated_rows = df[df['duplicated_partner_id'] == True]

    if duplicated_rows.shape[0] > 0:
        df.drop_duplicates(subset='partner_id', inplace=True)
        df.reset_index(drop=True, inplace=True)

    df.drop(columns=['duplicated_partner_id',
                    'partner_role',
                    'phone_number',
                    'fax_number',
                    'legal_form',
                    'web_address'], inplace=True)

    dt_cols = ['dt_created_at', 'dt_changed_at']

    for col in dt_cols:
        df[col] = pd.to_datetime(df[col].astype(str), format='%Y%m%d')

    return df