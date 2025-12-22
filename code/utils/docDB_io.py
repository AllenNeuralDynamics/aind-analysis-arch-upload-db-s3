import logging

# --------------
"""This is important to fix the `TypeError: sequence item 1: expected str instance, NoneType found` error
when use MetadataDbClien + IAM role to upsert record to docDB!!!
"""
import os
os.environ["AWS_DEFAULT_REGION"] = "us-west-2"
# --------------

from aind_data_access_api.document_db import MetadataDbClient

logger = logging.getLogger(__name__)

# Set up docDB client
PROJECT = "dynamic-foraging-model-fitting"
prod_db_client = MetadataDbClient(
    host="api.allenneuraldynamics.org",
    database="analysis",
    collection=PROJECT,
)


def upload_docDB_record_to_prod(dict_to_docDB, skip_already_exists):
    """Upload a result dictionary to the production DocumentDB.

    Parameters
    ----------
    result_dict : _type_
        _description_
    skip_already_exists : bool, optional
        _description_, by default True
    """
    
    # Check if job hash already exists, if yes, log warning, but still insert
    existed_record = prod_db_client._get_records({"_id": dict_to_docDB["_id"]})
    
    if len(existed_record):
        logger.warning(f"Job hash {dict_to_docDB['_id']} already exists in docDB!")

        if skip_already_exists:
            logger.warning(f"-- Skipped upserting to docDB!")
            return "skipped -- already exists"

    # Otherwise, upsert the record (add a new record or update an existing one)
    resp_write = prod_db_client.upsert_one_docdb_record(dict_to_docDB)
    
    if resp_write.status_code == 200:
        return "upload docDB success"
    
    logger.error(f"Failed to upload docDB record: {resp_write.status_code} - {resp_write.text}")
    return "upload docDB failed"
