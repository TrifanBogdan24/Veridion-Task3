import csv
import os, shutil
import pandas as pd


def read_parquet_file(parquet_file_path: str, ):
    df = pd.read_parquet(parquet_file_path)

    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)

    return df


def desc_sort_columns_by_percentage_of_completness(data_frame):
    """
    The columns with more null/empty values should have bigger index.
    The smaller the rows of the empty rows of a column, the lower the index
    """
    missing_percentage = data_frame.isnull().mean()
    
    sorted_columns = missing_percentage.sort_values().index.tolist()
    
    # Create new data frame
    return data_frame[sorted_columns]


def filter_columns(data_frame):
    """
    Filter the columns, after they were sorted in descending order by the number of completed rows
    """
    selected_columns = [
        6,  # website_domain
        4,  # website_tld
        3,  # company_name 
        9, # main_country_code
        10, # main_region
        11, # main_city
        12, # company_commercial_names
        15, # primary_phone
        16 # phone_numbers
    ]


    selected_column_names = [data_frame.columns[i] for i in selected_columns if i < len(data_frame.columns)]


    # Create new data frame
    return data_frame[selected_column_names]



def asc_sort_rows_by_domain(data_frame):
    return data_frame.sort_values(by=data_frame.columns[0], ascending=True).reset_index(drop=True)

def convert_data_frame_to_csv(data_frame, csv_file_path: str):
    data_frame.to_csv(csv_file_path, index=False)


def main():
    if os.path.exists("output"):
        shutil.rmtree("output/")
    if os.path.exists("tmp"):
        shutil.rmtree("tmp/")
    
    os.mkdir("output/")
    os.mkdir("tmp/")


    original_df = read_parquet_file("veridion_entity_resolution_challenge.snappy.parquet")
    convert_data_frame_to_csv(original_df, "tmp/file_01-veridion_entity_resolution_challenge.csv")

    sorted_columns_df = desc_sort_columns_by_percentage_of_completness(original_df)
    convert_data_frame_to_csv(sorted_columns_df, "tmp/file_02_sorted_columns.csv")

    filtered_columns_df = filter_columns(sorted_columns_df)
    convert_data_frame_to_csv(filtered_columns_df, "tmp/file_03_filtered_columns.csv")

    sorted_rows_by_domain_df = asc_sort_rows_by_domain(filtered_columns_df)
    convert_data_frame_to_csv(sorted_rows_by_domain_df, "tmp/file_04_sorted_rows_by_domain.csv")


if __name__ == '__main__':
    main()