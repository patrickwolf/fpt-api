# fpt-api
A wrapper around shotgun_api3 to allow retrieval of query fields.

## Why this wrapper?

Unfortunately, the shotgun_api3 does not allow retrieval of query fields.
It's also not thread safe.

Even though you could have your custom logic to compute the query fields (which could be faster than this library),
this library is a simple way to get the query fields. It parallelizes the retrieval of query fields by using threads.

Note that we still wait for the query fields to be retrieved before returning the results.
A future version could work with futures to return the results as soon as they are available.

## Installation

```bash
    pip install git+https://github.com/ksallee/fpt-api.git
```

## Usage

```python
from fpt_api import FPT

fpt = FPT(
    "https://yourshotgunurl.com",
    script_name="your_script_name",
    api_key="your_script_key"
)
fpt_shot = fpt.find_one(
    "Shot",
    [["id", "is", 1234]],
    ["code", "sg_status_list", "sg_query_field"]
)
fpt_shots = fpt.find(
    "Shot",
    [["id", "in", [1234, 12345]]],
    ["code", "sg_status_list", "sg_query_field"]
)
```
