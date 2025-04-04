import csv
import os, shutil
import pandas as pd
import numpy as np


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
        0, # original_index
        39,  # website_domain
        40,  # website_tld
        # Perhars they will be necessary in the future
        # 4, # main_country_code
        # 6, # main_region
        # 8, # main_city
        1,  # company_name 
        3, # company_commercial_names
        23, # business_model
        25, # product_type
        30, # main_business_category
        31,  # main_industry
        32,  # main_sector
        34 # phone_numbers
    ]


    selected_column_names = [data_frame.columns[i] for i in selected_columns if i < len(data_frame.columns)]


    # Create new data frame
    return data_frame[selected_column_names]



def sort_rows(data_frame):
    # TODO: add a second criteria of sorting:
    # where the 'base_domain' are the same, sort in ascending order by the column 'compnay_name', after you lowered the character of this string and remove all empty spaces
    # as it is for the 'base_domain', the remaining empty lines of the 'company_name' should be at the end (of the block of the same base_domain)
    def clean_company_name(series):
        return series.str.lower().str.replace(" ", "", regex=True)

    # Sort using 'base_domain' first, then transformed 'company_name'
    sorted_df = data_frame.sort_values(
        by=['base_domain', 'company_name'],
        ascending=[True, True],
        na_position="last",
        key=lambda col: clean_company_name(col) if col.name == "company_name" else col
    )

    return sorted_df

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
    original_df.insert(0, 'original_index', range(len(original_df)))  # Adds a new first column of indices
    convert_data_frame_to_csv(original_df, "tmp/file_01-veridion_entity_resolution_challenge.csv")

    sorted_columns_df = desc_sort_columns_by_percentage_of_completness(original_df)
    convert_data_frame_to_csv(sorted_columns_df, "tmp/file_02_sorted_columns.csv")

    filtered_columns_df = filter_columns(original_df)
    base_domains = []
    for i in range(len(filtered_columns_df)):
        website_domain = filtered_columns_df.iloc[i]['website_domain']
        if website_domain is None or website_domain == "":
            base_domains.append(np.nan)
        else:
            base_domains.append(website_domain.split(".")[0])
    filtered_columns_df.insert(1, 'base_domain', base_domains)  # Adds a new second column for base_domain


    convert_data_frame_to_csv(filtered_columns_df, "tmp/file_03_filtered_columns.csv")

    sorted_rows_by_domain_df = sort_rows(filtered_columns_df)
    convert_data_frame_to_csv(sorted_rows_by_domain_df, "tmp/file_04_sorted_rows_by_domain.csv")


if __name__ == '__main__':
    main()