import pandas as pd


df_calls = pd.read_csv(f"data/contac_center_data.csv", sep=";")
df_renta = pd.read_csv("data/renta_por_hogar.csv", sep=";", header=0)
df_delitos = pd.read_csv("data/delitos_por_municipio.csv", encoding='latin1', sep=";", skiprows=4)


def pivot_dataframe(df):
    df['Producto'].fillna('Sin_Producto', inplace=True)
    df_pivoted = df.pivot_table(index='CP', columns='Producto', aggfunc='size', fill_value=0)
    # Set CP as column
    df_pivoted.reset_index(inplace=True)
    return df_pivoted


def check_null_values_renta(df):
    df.dropna(subset=['Total'], inplace=True)
    df_cleaned = df[df['Total'] != '.']
    # Convert error to NaN values
    df_cleaned['Total_numeric'] = pd.to_numeric(df_cleaned['Total'], errors='coerce')
    null_values = df_cleaned['Total_numeric'].isnull().sum()
    if null_values > 0:
        print(f"{null_values} null values were found in the dataset")
    df_cleaned = df_cleaned.drop('Total_numeric', axis=1)
    return df_cleaned


def split_column_value(df):
    df[['CP', 'Municipio']] = df['Municipios'].str.split(n=1, expand=True)
    df['CP'] = df['CP'].astype(int)
    df['Periodo'] = df['Periodo']
    df['Total_Renta'] = df['Total'].astype(float)
    df['Tipo_Renta'] = df['Indicadores de renta media y mediana']
    df = df.drop('Municipios', axis=1)
    df = df[['CP', 'Municipio', 'Tipo_Renta', 'Periodo', 'Total_Renta']]
    return df


def format_df(df):
    # Remove last 4 rows
    df_table = df.iloc[1:-5, 1:-1]
    df_header = df.iloc[:1, :]
    # From row 2 to not include MADRID (COMUNIDAD DE)
    municipios = df.iloc[1:-5, 0]
    case_type = df_header.columns.tolist()
    case_type_cleaned = [case for case in case_type if 'Unnamed' not in case]
    period = df_header.iloc[0].tolist()
    period_cleaned = [year for year in period if str(year) != 'nan']
    df_formatted = pd.DataFrame(columns=['Municipio', 'Tipo_Caso', 'Periodo', 'Total_Delitos'])
    df_formatted['Municipio'] = municipios

    filas_casos = []

    for index, row in df_formatted.iterrows():
        for tipo in case_type_cleaned:
            for year in ['2019', '2020']:
                filas_casos.append(
                    {'Municipio': row['Municipio'], 'Tipo_Delito': tipo, 'Periodo': year, 'Total_Delitos': None})

    df_formatted = pd.concat([df_formatted, pd.DataFrame(filas_casos)], ignore_index=True)
    df_formatted = df_formatted.dropna(subset=['Tipo_Delito', 'Periodo'])

    df_formatted.reset_index(drop=True, inplace=True)
    fill_values = df_table.values.flatten().tolist()
    df_formatted['Periodo'] = df_formatted['Periodo'].astype(int)
    df_formatted['Total_Delitos'] = fill_values
    df_formatted['Total_Delitos'] = df_formatted['Total_Delitos'].astype(float)
    df_formatted['Municipio'] = df_formatted['Municipio'].str.replace('- Municipio de ', '')
    df_formatted['Municipio'] = df_formatted['Municipio'].str.replace('MADRID \(COMUNIDAD DE\)', 'Madrid')
    df_formatted = df_formatted[['Municipio', 'Tipo_Delito', 'Periodo', 'Total_Delitos']]

    return df_formatted

def pivot_tabla_delitos(df):
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


def pivot_tabla_rentas(df):
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

def join_data(df_calls, df_renta, df_delitos):
    merge_calls_renta = pd.merge(df_renta, df_calls, on='CP', how='inner')
    merge_calls_renta_delitos = pd.merge(merge_calls_renta, df_delitos, on=['Municipio', 'Periodo'], how='left')
    print(merge_calls_renta_delitos.to_string())
    #print(merge_calls_renta_delitos.head(10).to_string())
    #return merge_calls_renta_delitos


def unify_periodo(df):
    df_test = df[df['Periodo_Renta'] == 2015]
    #print(df_test.groupby('Periodo_Renta')['Total_Renta'].mean())
    pass


if __name__ == '__main__':
    df_calls_pivoted = pivot_dataframe(df_calls)
    df_renta_cleaned = check_null_values_renta(df_renta)
    df_renta_split = split_column_value(df_renta_cleaned)
    df_delitos_formatted = format_df(df_delitos)
    df_delitos_pivoted = pivot_tabla_delitos(df_delitos_formatted)
    df_renta_pivoted = pivot_tabla_rentas(df_renta_split)
    df_joint = join_data(df_calls_pivoted, df_renta_pivoted, df_delitos_pivoted)
    #print(df_renta_split[df_renta_split['Periodo_Renta'] == 2015])
    #unify_periodo(df_joint)
    #check_null_values_contac()