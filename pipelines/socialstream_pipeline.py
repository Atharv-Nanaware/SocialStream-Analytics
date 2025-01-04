

import pandas as pd

from etls.socialstream_etl import connect_reddit, extract_posts, transform_data, load_data_to_csv
from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH


def socialstream_pipeline(file_name: str, subreddit: str, time_filter='all', limit=None):
    # connecting to instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'My_Project_Atharv01')
    # extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    # transformation
    post_df = transform_data(post_df)
    # loading to csv
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)

    return file_path