import json
import ctypes
from dask.distributed import Client


def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)


def PA0(user_reviews_csv_path):
    client = Client()
    # Helps fix any memory leaks. We noticed Dask can slow down when same code is run again.
    client.run(trim_memory)
    client = client.restart()

    # WRITE YOUR CODE HERE

    submit = <YOUR_USERS_DATAFRAME > .describe().compute().round(2)
    with open('results_PA0.json', 'w') as outfile:
        json.dump(json.loads(submit.to_json()), outfile)
