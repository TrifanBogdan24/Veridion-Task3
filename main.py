import csv
import os, shutil
import pandas as pd
import numpy as np
from typing import Dict, List
import re


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


    """
    I also used in my last querries:
    3, # company_commercial_names
    4, # main_country_code
    6, # main_region
    8, # main_city
    23, # business_model
    25, # product_type
    """
    selected_columns = [
        0, # index_reference
        39,  # website_domain
        40,  # website_tld
        1,  # company_name
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





def normalize_text(text: str) -> str:
    """Lowercase, strip spaces, and remove special characters for comparison"""
    return re.sub(r'\s+', '', text.lower()) if isinstance(text, str) else ''


def distinguish_companies(sorted_and_filtered_df, original_df):
    """
    Return a dictionary: to each base domain is mapped a list of indices for the lines in the original tables
    """
    companies: Dict[str, List[int]] = {}
    existing_mappings = {}
    
    # Preprocess original_df to store existing mappings
    for _, row in original_df.iterrows():
        index = row.name  # Assuming index is unique
        base_domain = row.get('base_domain', '')
        
        if not base_domain or pd.isna(base_domain):
            continue
        
        normalized_values = {
            normalize_text(str(row.get(col, '')))
            for col in [
                'company_name', 'company_commercial_names', 'main_address', 'phone_numbers',
                'all_domains', 'facebook_url', 'linked_url', 'instagram_url',
                'primary_email', 'emails', 'other_emails', 'youtube_url',
                'android_app_url', 'ios_app_url', 'tiktok_url'
            ] if row.get(col)  # Ensure non-empty values
        }
        
        existing_mappings[index] = (base_domain, normalized_values)
    
    # Process sorted_and_filtered_df
    for _, row in sorted_and_filtered_df.iterrows():
        base_domain: str = row['base_domain']
        index_reference: List[int] = row['index_reference']
        
        if not base_domain or base_domain == "" or pd.isna(base_domain):
            """
            The part of the code finds to which company to assign an entry from the table;
            entry's 'base_domain' field has not been completed.
            It will try to 'match' other relevant entries, to check where the reference's data has appeared before
            
            original_df contains the following relevant: column names:
            - company_name
            - company_commercial_names (values separated by `|` pipe operator)
            - main_address
            - phone_numbers (values separated by `|` pipe operator)
            - main_latidune and main_longitude (they work together)
            - all_domains (values separated by `|` pip operator)
            - facebook_url
            - linked_url
            - instagram_url
            - primary_email
            - emails (values separated by `|` pip operator)
            - other_emails (values separated by `|` pip operator)
            - youtube_url
            - android_app_url
            - ios_app_url
            - tiktok_url

            If one of the field of the index_references's rows matches
            the field of an index (already associated to a domain in the dictionary),
            add it to that specific domain

            Company names and commercial names are compared by storing the values of the entries,
            lowering the strings, removing the empty spaces and lexico-graphically comparing them
            """
            matched_domain = None
            for col in [
                'company_name', 'company_commercial_names', 'main_address', 'phone_numbers',
                'all_domains', 'facebook_url', 'linked_url', 'instagram_url',
                'primary_email', 'emails', 'other_emails', 'youtube_url',
                'android_app_url', 'ios_app_url', 'tiktok_url'
            ]:
                row_value = normalize_text(str(row.get(col, '')))
                if not row_value:
                    continue
                
                for existing_index, (existing_domain, existing_values) in existing_mappings.items():
                    if row_value in existing_values:
                        matched_domain = existing_domain
                        break
                
                if matched_domain:
                    break
            
            if matched_domain:
                companies.setdefault(matched_domain, []).append(index_reference)
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
    # For visual debug: original_df.to_csv("tmp/file_01-veridion_entity_resolution_challenge.csv", index=False)


    # For visual debug:
    sorted_columns_df = desc_sort_columns_by_percentage_of_completness(original_df)
    # For visual debug: sorted_columns_df.to_csv("tmp/file_02_sorted_columns.csv", index=False)


    filtered_columns_df = filter_columns(original_df)

    base_domains = []
    for i in range(len(filtered_columns_df)):
        website_domain = filtered_columns_df.iloc[i]['website_domain']
        if website_domain is None or website_domain == "":
            base_domains.append(np.nan)
        else:
            base_domains.append(website_domain.split(".")[0])
    filtered_columns_df.insert(1, 'base_domain', base_domains)  # Adds a new second column for base_domain


    # For visual debug: filtered_columns_df.to_csv("tmp/file_03_filtered_columns.csv", index=False)

    sorted_rows_by_domain_df = sort_rows(filtered_columns_df)
    del filtered_columns_df
    # For visual debug: sorted_rows_by_domain_df.to_csv("tmp/file_04_sorted_rows_by_domain.csv", index=False)

    companies: Dict[str, List[int]] = distinguish_companies(sorted_rows_by_domain_df, original_df)
    del sorted_rows_by_domain_df
    sorted_domains = sorted(companies.keys())
    

    df_uniques = pd.DataFrame(columns=['base_domain', 'index_reference'])
    df_duplicates = pd.DataFrame(columns=['base_domain', 'indices_reference'])

    # Classify domains as unique or duplicates
    for domain in sorted_domains:
        ref_indices: List[int] = companies.get(domain)
        if len(ref_indices) == 1:
            df_uniques = pd.concat([df_uniques, pd.DataFrame({'base_domain': [domain], 'index_reference': ref_indices})], ignore_index=True)
        else:
            df_duplicates = pd.concat([df_duplicates, pd.DataFrame({'base_domain': [domain], 'indices_reference': [ref_indices]})], ignore_index=True)


    df_uniques.to_parquet("output/uniques.parquet")
    # For visual debug: df_uniques.to_csv("tmp/file_05_uniques.csv", index=False)

    df_duplicates.to_parquet("output/duplicates.parquet")
    # For visual debug: df_duplicates.to_csv("tmp/file_06_duplicates.csv", index=False)


if __name__ == '__main__':
    main()
