import time
import json
import dask.dataframe as dd
from dask.distributed import Client
import ctypes

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

    # WRITE YOUR CODE HERE   
            
    # assign to each variable the answer to your question. 
    # answers must follow the datatype mentioned
    ans1 = None        
    ans2 = None
    ans3 = None
    ans4 = None
    ans5 = None
    ans6 = None
    ans7 = None
 
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
    