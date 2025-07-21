def standardize_column_names(df):
    """
    Strip whitespace, lowercase, and replace spaces with underscores.
    """
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
    )
    return df

def standardize_asymmetrique_indexes(df):
    """
    Fix and map values in Asymmetrique index columns.
    """
    for col in ['Asymmetrique Activity Index', 'Asymmetrique Profile Index']:
        df[col] = (
            df[col]
            .astype(str)
            .str.split('.', expand=True)[0]
            .replace({'01': '3', '02': '2', '03': '1'})
            .astype(float)
        )
    return df
