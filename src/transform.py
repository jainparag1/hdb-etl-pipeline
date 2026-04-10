def generate_identifier(df):
    import re

    # Block digits
    df['block_digits'] = df['block'].apply(lambda x: re.sub(r'\D', '', str(x)).zfill(3)[:3])

    # Avg resale price grouping
    avg_price = df.groupby(['month', 'town', 'flat_type'])['resale_price'].mean().reset_index()
    avg_price['price_prefix'] = avg_price['resale_price'].astype(int).astype(str).str[:2]

    df = df.merge(avg_price[['month','town','flat_type','price_prefix']], on=['month','town','flat_type'])

    df['month_num'] = df['month'].str[-2:]
    df['town_char'] = df['town'].str[0]

    df['resale_id'] = (
        "S" +
        df['block_digits'] +
        df['price_prefix'] +
        df['month_num'] +
        df['town_char']
    )

    return df