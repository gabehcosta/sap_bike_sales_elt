import pandas as pd

def transform(raw_data) -> pd.DataFrame:
    df = pd.read_csv(raw_data)

    df.columns = df.columns.str.lower()

    cols_rename = {
        'employeeid': 'employee_id',
        'name_first': 'first_name',
        'name_middle': 'middle_name',
        'name_last': 'last_name',
        'name_initials': 'initials',
        'phonenumber': 'phone_number',
        'emailaddress': 'email_address',
        'loginname': 'login_name',
        'addressid': 'address_id',
        'validity_startdate': 'dt_start_validity',
        'validity_enddate': 'dt_end_validity'
    }

    df.rename(columns=cols_rename, inplace=True)

    df['duplicated_employee_id'] = df.duplicated(subset='employee_id', keep=False)
    duplicated_rows = df[df['duplicated_employee_id'] == True]

    if duplicated_rows.shape[0] > 0:
        df.drop_duplicates(subset='employee_id', inplace=True)
        df.reset_index(drop=True, inplace=True)

    str_cols = [
        'first_name',
        'middle_name',
        'last_name',
        'sex',
        'language',
        'email_address',
        'login_name'
    ]

    for col in str_cols:
        df[col] = df[col].str.strip()

    df.fillna({'middle_name': '-'}, inplace=True)

    df['full_name'] = df.apply(
        lambda row: f'{row["first_name"]} ' +
                    (f'{row["middle_name"]}. ' if row['middle_name']!= '-' else '') +
                    row['last_name'], axis=1 
    )
    df['name_initials'] = df.apply(
        lambda row: (
            row['first_name'][0].upper() + '.' +
            (row['middle_name'][0].upper()+ '.' if row['middle_name'] != '-' else '') +
            row['last_name'][0].upper() + '.'
        ), axis=1
    )

    df['sex'] = df['sex'].replace({
        'M': 'Male',
        'F': 'Female'
    })

    df.rename(columns={'sex':'gender'}, inplace=True)

    df.drop(columns=['duplicated_employee_id',
                    'dt_start_validity',
                    'dt_end_validity',
                    'phone_number',
                    'initials',
                    'middle_name',
                    'language',
                    'unnamed: 13',
                    'unnamed: 14',
                    'unnamed: 15',
                    'unnamed: 16',
                    'unnamed: 17',
                    'unnamed: 18'], inplace=True)

    return df