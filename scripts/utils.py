import os


def write_to_csv(df, filename):
    """
    Save DataFrame to a CSV file in the 'output' folder
    """
    output_folder = '../output'
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    filepath = os.path.join(output_folder, filename)
    df.to_csv(filepath, index=False, encoding='latin1')
    print(f"DataFrame saved to '{filepath}'")