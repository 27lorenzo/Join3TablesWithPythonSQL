import pandas as pd


def join_data(df_calls, df_renta, df_delitos):
    """
    Join three tables
    """
    merge_calls_renta = pd.merge(df_renta, df_calls, on='CP', how='inner')
    merge_calls_renta_delitos = pd.merge(merge_calls_renta, df_delitos, on=['Municipio', 'Periodo'], how='left')
    merge_calls_renta_delitos.fillna(0, inplace=True)
    merge_calls_renta_delitos.sort_values(by=['CP', 'Municipio', 'Periodo'], ascending=[True, True, False], inplace=True)

    return merge_calls_renta_delitos
