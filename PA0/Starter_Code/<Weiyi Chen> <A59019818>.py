import numpy as np
import time
import dask.dataframe as dd
import dask
import pandas as pd
import json
import ctypes
from dask.distributed import Client


def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)


def PA0(user_reviews_csv_path):
    client = Client()
    client.run(trim_memory)
    client.restart()
    user_reviews_ddf = dd.read_csv(user_reviews_csv_path)
    number_products_rated = user_reviews_ddf.groupby(
        "reviewerID", sort=False).asin.count(split_out=100)
    avg_ratings = user_reviews_ddf.groupby(
        "reviewerID", sort=False).overall.mean(split_out=100)
    user_reviews_ddf['year'] = user_reviews_ddf.map_partitions(
        lambda df: df.reviewTime.apply(lambda x: int(x[-4:])), meta=pd.Series(dtype=int))
    reviewing_since = user_reviews_ddf.groupby(
        'reviewerID', sort=False).year.min(split_out=100)

    user_reviews_ddf['helpful_votes'] = user_reviews_ddf.map_partitions(lambda df: df.helpful.apply(
        lambda x: x[1:-1].split(',')[0]).astype(int), meta=pd.Series(dtype=int))
    user_reviews_ddf['total_votes'] = user_reviews_ddf.map_partitions(lambda df: df.helpful.apply(
        lambda x: x[1:-1].split(',')[1]).astype(int), meta=pd.Series(dtype=int))

    helpful_votes = user_reviews_ddf.groupby(
        'reviewerID', sort=False).helpful_votes.sum(split_out=100)
    total_votes = user_reviews_ddf.groupby(
        'reviewerID', sort=False).total_votes.sum(split_out=100)

    dd_res = dd.concat([number_products_rated, avg_ratings,
                       reviewing_since, helpful_votes, total_votes], axis=1)
    dd_res = dd_res.rename(columns={
                           'asin': 'number_products_rated', 'overall': 'avg_ratings', 'year': 'reviewing_since'})
    submit = dd_res.describe().compute().round(2)
    with open('results_PA0.json', 'w') as outfile:
        json.dump(json.loads(submit.to_json()), outfile)
    return submit


submit = PA0("user_reviews_Release.csv")
