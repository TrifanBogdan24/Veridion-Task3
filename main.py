import csv
import os, shutil
import pandas as pd
import numpy as np
from typing import Dict, List


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
        0, # index_reference
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
    """
    Sorts:
    - in ascending order by the 'base_domain' field
    - places all empty 'base_domain' entries at the end of the data frame
    - when two entries have the same 'base_domain':
        - in ascending order by the 'compnay_name' (after the string was lowered and removed empty spaces)
        - places all empty 'company_name's at the end of the group
    """
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


def distinguish_companies(data_frame):
    companies: Dict[str, List[int]] = {}
    
    for _, row in data_frame.iterrows():
        base_domain = row['base_domain']
        index_reference = row['index_reference']
        
        if not base_domain or base_domain == "" or pd.isna(base_domain):
            # TODO: implement later
            continue
        
        if base_domain not in companies:
            companies[base_domain] = [index_reference]
        else:
            companies[base_domain].append(index_reference)
    
    return companies


def main():
    if os.path.exists("output"):
        shutil.rmtree("output/")
    if os.path.exists("tmp"):
        shutil.rmtree("tmp/")
    
    os.mkdir("output/")
    os.mkdir("tmp/")


    original_df = read_parquet_file("veridion_entity_resolution_challenge.snappy.parquet")
    original_df.insert(0, 'index_reference', range(len(original_df)))  # Adds a new first column of indices
    original_df.to_csv("tmp/file_01-veridion_entity_resolution_challenge.csv", index=False)

    sorted_columns_df = desc_sort_columns_by_percentage_of_completness(original_df)
    sorted_columns_df.to_csv("tmp/file_02_sorted_columns.csv", index=False)
    del sorted_columns_df

    filtered_columns_df = filter_columns(original_df)
    del original_df

    base_domains = []
    for i in range(len(filtered_columns_df)):
        website_domain = filtered_columns_df.iloc[i]['website_domain']
        if website_domain is None or website_domain == "":
            base_domains.append(np.nan)
        else:
            base_domains.append(website_domain.split(".")[0])
    filtered_columns_df.insert(1, 'base_domain', base_domains)  # Adds a new second column for base_domain


    filtered_columns_df.to_csv("tmp/file_03_filtered_columns.csv", index=False)

    sorted_rows_by_domain_df = sort_rows(filtered_columns_df)
    del filtered_columns_df
    sorted_rows_by_domain_df.to_csv("tmp/file_04_sorted_rows_by_domain.csv", index=False)

    companies: Dict[str, List[int]] = distinguish_companies(sorted_rows_by_domain_df)
    del sorted_rows_by_domain_df
    sorted_domains = sorted(companies.keys())
    

    df_uniques = pd.DataFrame(columns=['base_domain', 'index_reference'])
    df_duplicates = pd.DataFrame(columns=['base_domain', 'indices_reference'])

    # Classify domains as unique or duplicates
    for domain in sorted_domains:
        indices: List[int] = companies.get(domain)
        
        if len(indices) == 1:
            df_uniques = pd.concat([df_uniques, pd.DataFrame({'base_domain': [domain], 'index_reference': indices})], ignore_index=True)
        else:
            df_duplicates = pd.concat([df_duplicates, pd.DataFrame({'base_domain': [domain], 'indices_reference': [indices]})], ignore_index=True)


    df_uniques.to_parquet("output/uniques.parquet")
    df_uniques.to_csv("tmp/file_05_uniques.csv", index=False)

    df_duplicates.to_parquet("output/duplicates.parquet")
    df_duplicates.to_csv("tmp/file_06_duplicates.csv", index=False)


if __name__ == '__main__':
    main()