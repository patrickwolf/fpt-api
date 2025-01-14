# fpt-api
A wrapper around shotgun_api3 to allow retrieval of query fields.

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
