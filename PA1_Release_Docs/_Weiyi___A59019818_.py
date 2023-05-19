#!/usr/bin/env python
# coding: utf-8

# In[16]:


import time
import json
import dask.dataframe as dd
from dask.distributed import Client
import ctypes
import pandas as pd

import numpy as np
import time
import dask.dataframe as dd
import dask
import pandas as pd
import json
import ctypes
from dask.distributed import Client  


def trim_memory() -> int:
    """
    helps to fix any memory leaks.
    """
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)
         
def PA1(reviews_csv_path,products_csv_path):
    start = time.time()
    client = Client('127.0.0.1:8786')
    client.run(trim_memory)
    client = client.restart()
    print(client)
    
    reviews_ddf = dd.read_csv(reviews_csv_path)
    products_ddf = dd.read_csv(products_csv_path, dtype={'asin': 'object','categories':'str'})
    
    # Q1 
    percentage_1 = reviews_ddf.isna().mean()*100
    
    # Q2. 
    percentage_2 = products_ddf.isna().mean()*100
    
    # Q3. 
    total_ddf = reviews_ddf.rename(columns={'asin': 'rasin'})[['rasin', 'overall']].merge(products_ddf.rename(columns={'asin': 'pasin'})[['pasin', 'price']], how='left', 
                                                      left_on='rasin', right_on='pasin')
    
    # Q4. 
    price_stats = products_ddf['price'].describe()
    
    # Q5 
#     def f(srs):
#         return srs.map(lambda x: "" if pd.isna(x) else eval(x)[0][0])

#     products_ddf['super_cate'] = products_ddf['categories'].map_partitions(f, meta=pd.Series(dtype=str))
#     dictionary = products_ddf.groupby("super_cate", sort=False).asin.count(split_out=100)
    
    products_ddf['categories'] = products_ddf['categories'].fillna("[['']]")  
    def f(categories):
        return eval(categories)[0][0]
    products_ddf['cate'] = products_ddf['categories'].map(f, meta=pd.Series(dtype=str))
    dictionary = products_ddf.groupby("cate", sort=False).asin.count(split_out=100)
    
    ans1, ans2, total_ddf, price_stats, dictionary = dd.compute(percentage_1, percentage_2, total_ddf, price_stats, dictionary)
    
    ans1 = round(ans1, 2).to_dict()
    ans2 = round(ans2, 2).to_dict()
    
    ans3 = (total_ddf.overall).corr(total_ddf.price).tolist()
    ans3 = round(ans3,2)
    ans4 = {'mean': round(price_stats['mean'],2), 'std': round(price_stats['std'],2), 'medium': round(price_stats['50%'],2), 'min': round(price_stats['min'],2), 'max': round(price_stats['max'],2)}
    ans5 = dictionary.sort_values(ascending=False).to_dict() 
    del ans5[""]
    
    # Q6. 
    dangling6 = total_ddf['pasin'].isna().sum()
    ans6 = 1 if dangling6 > 0 else 0

    # Q7 
    related = products_ddf['related'].str.extractall('\'([A-Z0-9]+)\'')
    p_asin = set(products_ddf['asin'].unique())
    related_asin = set(related)
    dangling7 = related_asin - p_asin
    ans7 = 1 if dangling7 else 0
 
    # DO NOT MODIFY
    assert type(ans1) == dict, f"answer to question 1 must be a dictionary like {{'reviewerID':0.2, ..}}, got type = {type(ans1)}"
    assert type(ans2) == dict, f"answer to question 2 must be a dictionary like {{'asin':0.2, ..}}, got type = {type(ans2)}"
    assert type(ans3) == float, f"answer to question 3 must be a float like 0.8, got type = {type(ans3)}"
    assert type(ans4) == dict, f"answer to question 4 must be a dictionary like {{'mean':0.4,'max':0.6,'median':0.6...}}, got type = {type(ans4)}"
    assert type(ans5) == dict, f"answer to question 5 must be a dictionary, got type = {type(ans5)}"         
    assert ans6 == 0 or ans6==1, f"answer to question 6 must be 0 or 1, got value = {ans6}" 
    assert ans7 == 0 or ans7==1, f"answer to question 7 must be 0 or 1, got value = {ans7}" 
    
    end = time.time()
    runtime = end-start
    print(f"runtime  = {runtime}s")
    ans_dict = {
        "q1": ans1,
        "q2": ans2,
        "q3": ans3,
        "q4": ans4,
        "q5": ans5,
        "q6": ans6,
        "q7": ans7,
        "runtime": runtime
    }
    with open('results_PA1.json', 'w') as outfile: json.dump(ans_dict, outfile)
    return runtime  


# In[ ]:





# In[ ]:




