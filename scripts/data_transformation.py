import pandas as pd


def pivot_df_calls(df):
    """
    Pivot df_calls by 'Producto' type and count number of calls per each
    """
    df_pivoted = df.pivot_table(index='CP', columns='Producto', aggfunc='size', fill_value=0)
    df_pivoted.reset_index(inplace=True)

    return df_pivoted


def split_column_renta(df):
    """
        Split column 'Municipios' into 'CP' and 'Municipio' to later join by them
    """

    df[['CP', 'Municipio']] = df['Municipios'].str.split(n=1, expand=True)
    df['CP'] = df['CP'].astype(int)
    df['Total_Renta'] = df['Total'].astype(float)
    df['Periodo'] = df['Periodo'].astype(int)
    df['Tipo_Renta'] = df['Indicadores de renta media y mediana']
    df = df.drop('Municipios', axis=1)
    df = df[['CP', 'Municipio', 'Tipo_Renta', 'Periodo', 'Total_Renta']]
    return df


def pivot_df_rentas(df):
    """
        Pivot df_rentas table to get 'Total_Renta' for each 'Tipo_Renta' per combination of 'CP', 'Municpio' and 'Periodo'
    """
    df_pivot = df.pivot_table(values='Total_Renta', index=['CP', 'Municipio', 'Periodo'], columns='Tipo_Renta')
    df_pivot.reset_index(inplace=True)
    columns_order = ['CP',
                     'Municipio',
                     'Periodo',
                     'Renta neta media por persona ',
                     'Renta neta media por hogar',
                     'Media de la renta por unidad de consumo',
                     'Mediana de la renta por unidad de consumo',
                     'Renta bruta media por persona',
                     'Renta bruta media por hogar']
    df_ordered = df_pivot[columns_order]

    return df_ordered


def format_df_delitos(df):
    """
        Extract data from csv and load it into a new dataframe
    """
    # Remove last 4 rows and split into two dataframes: header and content
    df_table = df.iloc[1:-5, 1:-1]
    df_header = df.iloc[:1, :]
    # Extract list of municipios
    municipios = df.iloc[1:-5, 0]
    # Extract list of tipos de delitos
    tipo_delito = df_header.columns.tolist()
    tipo_delito_cleaned = [case for case in tipo_delito if 'Unnamed' not in case]
    # Create new dataframe to which append the csv data
    df_formatted = pd.DataFrame(columns=['Municipio', 'Tipo_Delito', 'Periodo', 'Total_Delitos'])
    df_formatted['Municipio'] = municipios

    filas_casos = []

    for index, row in df_formatted.iterrows():
        for tipo in tipo_delito_cleaned:
            for year in ['2019', '2020']:
                filas_casos.append(
                    {'Municipio': row['Municipio'], 'Tipo_Delito': tipo, 'Periodo': year, 'Total_Delitos': None})

    df_formatted = pd.concat([df_formatted, pd.DataFrame(filas_casos)], ignore_index=True)
    df_formatted = df_formatted.dropna(subset=['Tipo_Delito', 'Periodo'])
    df_formatted.reset_index(drop=True, inplace=True)
    # Create a list from the content's df created at the beginning of this function and fill the new df
    fill_values = df_table.values.flatten().tolist()
    df_formatted['Total_Delitos'] = fill_values
    # Format columns and values
    df_formatted['Total_Delitos'] = df_formatted['Total_Delitos'].astype(float)
    df_formatted['Periodo'] = df_formatted['Periodo'].astype(int)
    df_formatted['Municipio'] = df_formatted['Municipio'].str.replace('- Municipio de ', '')
    df_formatted['Municipio'] = df_formatted['Municipio'].str.replace('MADRID \(COMUNIDAD DE\)', 'Madrid')
    df_formatted = df_formatted[['Municipio', 'Tipo_Delito', 'Periodo', 'Total_Delitos']]

    return df_formatted


def pivot_df_delitos(df):
    """
        Pivot new df_delitos table to get 'Total_Delitos' for each 'Tipo_Delito' per combination of 'Municpio' and 'Periodo'
    """
    df_pivot = df.pivot_table(values='Total_Delitos', index=['Municipio', 'Periodo'], columns='Tipo_Delito')
    df_pivot.reset_index(inplace=True)

    columns_order = ['Municipio',
                     'Periodo',
                    '1.-Homicidios dolosos y asesinatos consumados',
                    '2.-Homicidios dolosos y asesinatos en grado tentativa',
                    '3.-Delitos graves y menos graves de lesiones y riña tumultuaria',
                    '4.-Secuestro', '5.-Delitos contra la libertad e indemnidad sexual',
                    '5.1.-Agresión sexual con penetración',
                    '5.2.-Resto de delitos contra la libertad e indemnidad sexual',
                    '6.-Robos con violencia e intimidación',
                    '7.- Robos con fuerza en domicilios, establecimientos y otras instalaciones',
                    '7.1.-Robos con fuerza en domicilios', '8.-Hurtos',
                    '9.-Sustracciones de vehículos',
                    '10.-Tráfico de drogas',
                    'Resto de infracciones penales',
                    'TOTAL INFRACCIONES PENALES']

    df_ordered = df_pivot[columns_order]

    return df_ordered
