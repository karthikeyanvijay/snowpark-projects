CREATE OR REPLACE PROCEDURE sp_load_elasticsearch(index_name STRING,
                                                   table_name STRING,
                                                   num_threads INT DEFAULT 1,
                                                   pipeline_name STRING DEFAULT '')
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (elastic_api_access_integration)
PACKAGES = ('snowflake-snowpark-python','requests','elasticsearch','pandas', 'joblib')
SECRETS = ('elastic_api_key' = elastic_api_key
          , 'elastic_cloud_id' = elastic_cloud_id
          , 'elastic_endpoint' = elastic_endpoint)
AS
$$
import _snowflake
import joblib
joblib.parallel_backend("loky")
from joblib import Parallel, delayed
import pandas as pd
import json
from datetime import datetime, timezone
from elasticsearch import Elasticsearch, helpers


def bulk_index_data(
    index_name, pipeline_name, data, api_key, cloud_id, elasticsearch_endpoint
):
    es = Elasticsearch(
        [elasticsearch_endpoint],
        api_key=(api_key),
        cloud_id=cloud_id,
        use_ssl=True,
        verify_certs=True,
    )
    actions = [
        {
            "_index": index_name,
            "_source": document,
            **({"pipeline": pipeline_name} if pipeline_name != "" else {}),
        }
        for document in data
    ]
    successes, api_call_details = helpers.bulk(es, actions)
    return successes, api_call_details


def batch_processing(
    thread_num,
    batch_num,
    pandas_df,
    index_name,
    pipeline_name,
    api_key,
    cloud_id,
    elasticsearch_endpoint,
):
    start_time = datetime.now(timezone.utc)
    data = [json.loads(j) for j in pandas_df["JSON_TEXT"]]
    successes, api_call_details = bulk_index_data(
        index_name, pipeline_name, data, api_key, cloud_id, elasticsearch_endpoint
    )
    completed_at_utc = datetime.now(timezone.utc)
    batch_time_taken_seconds = (completed_at_utc - start_time).total_seconds()
    return {
        "thread_num": thread_num,
        "batch_num": batch_num,
        "completed_at_utc": completed_at_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "batch_time_taken_seconds": batch_time_taken_seconds,
        "succeeded_record_count": successes,
        "api_call_details": str(
            api_call_details
        ), 
    }


def main(session, index_name, table_name, num_threads=1, pipeline_name=""):

    df = session.table(table_name).selectExpr("OBJECT_CONSTRUCT(*) as JSON_TEXT")

    api_key = _snowflake.get_generic_secret_string("elastic_api_key")
    cloud_id = _snowflake.get_generic_secret_string("elastic_cloud_id")
    elasticsearch_endpoint = _snowflake.get_generic_secret_string("elastic_endpoint")

    results = Parallel(n_jobs=num_threads)(
        delayed(batch_processing)(
            i % num_threads + 1, 
            i + 1, 
            batch,
            index_name,
            pipeline_name,
            api_key,
            cloud_id,
            elasticsearch_endpoint,
        )
        for i, batch in enumerate(df.to_pandas_batches())
    )

    results_df = session.create_dataframe(
        pd.DataFrame(results),
        schema=[
            "thread_num",
            "batch_num",
            "completed_at_utc",
            "batch_time_taken_seconds",
            "succeeded_record_count",
            "api_call_details",
        ],
    )
    return results_df
$$;