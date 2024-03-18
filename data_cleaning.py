import pandas as pd

def check_null_values_calls(df):
    """
    Check if there are NULL values in df and fill na values in 'Producto' with 'Sin_Producto'
    """
    print(df.isna().any())
    df['Producto'].fillna('Sin_Producto', inplace=True)

    return df

def check_null_values_renta(df):
    """
    Check if there are NULL values in df and remove invalid rows
    """
    print(df.isna().any())
    df.dropna(subset=['Total'], inplace=True)
    df_cleaned = df[df['Total'] != '.']

    return df_cleaned
