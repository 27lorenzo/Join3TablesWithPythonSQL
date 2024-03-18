import pandas as pd
from data_cleaning import check_null_values_calls, check_null_values_renta
from data_transformation import pivot_df_calls, split_column_renta, pivot_df_rentas, format_df_delitos, pivot_df_delitos
from data_joining import join_data
from utils import write_to_csv

if __name__ == '__main__':
    df_calls = pd.read_csv(f"data/contac_center_data.csv", sep=";")
    df_renta = pd.read_csv("data/renta_por_hogar.csv", sep=";", header=0)
    df_delitos = pd.read_csv("data/delitos_por_municipio.csv", encoding='latin1', sep=";", skiprows=4)

    df_calls_cleaned = check_null_values_calls(df_calls)
    df_calls_pivoted = pivot_df_calls(df_calls_cleaned)
    df_renta_cleaned = check_null_values_renta(df_renta)
    df_renta_split = split_column_renta(df_renta_cleaned)
    df_renta_pivoted = pivot_df_rentas(df_renta_split)
    df_delitos_cleaned = format_df_delitos(df_delitos)
    df_delitos_pivoted = pivot_df_delitos(df_delitos_cleaned)

    df_final = join_data(df_calls_pivoted, df_renta_pivoted, df_delitos_pivoted)
    write_to_csv(df_final, 'df_final.csv')

    print(df_final.to_string())
