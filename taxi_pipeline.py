"""REST API source for NYC taxi data.

API: https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api
- Paginated JSON (1,000 records per page)
- Stop when empty page is returned
"""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def taxi_pipeline_rest_api_source():
    """Define dlt resources from the NYC taxi REST API."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/",
        },
        "resource_defaults": {
            "write_disposition": "replace",
            "endpoint": {
                "params": {
                    "limit": 1000,
                },
            },
        },
        "resources": [
            {
                "name": "trips",
                "endpoint": {
                    "path": "data_engineering_zoomcamp_api",
                    "method": "GET",
                    "data_selector": "$",
                    "paginator": {
                        "type": "offset",
                        "limit": 1000,
                        "offset": 0,
                        "offset_param": "offset",
                        "limit_param": "limit",
                        "total_path": None,
                        "stop_after_empty_page": True,
                        "maximum_offset": 1000,
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline_rest_api_source())
    print(load_info)
