import logging
import time
from functools import wraps
from pymongo.errors import ServerSelectionTimeoutError
from sshtunnel import HandlerSSHTunnelForwarderError

SSH_ERRORS = [ServerSelectionTimeoutError, HandlerSSHTunnelForwarderError]

import traceback
from datetime import datetime

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
PROJECT = "dynamic-foraging-analysis"
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
            logger.warning(f"-- Skipped!")
            return "skipped -- already exists"

    # Otherwise, upsert the record (add a new record or update an existing one)
    resp_write = prod_db_client.upsert_one_docdb_record(dict_to_docDB)
    
    if resp_write.status_code == 200:
        return "upload docDB success"
    
    logger.error(f"Failed to upload docDB record: {resp_write.status_code} - {resp_write.text}")
    return "upload docDB failed"

def insert_result_to_docDB_ssh(result_dict, collection_name, doc_db_client, skip_already_exists=True) -> dict:
    """_summary_

    Parameters
    ----------
    result_dict : dict
        bson-compatible result dictionary to be inserted
    collection_name : _type_
        name of the collection to insert into (such as "mle_fitting")

    Returns
    -------
    dict
        docDB upload status
    """
    doc_db_client.collection_name = collection_name
    db = doc_db_client.collection

    # Check if job hash already exists, if yes, log warning, but still insert
    if_exists = db.find_one({"job_hash": result_dict["job_hash"]})
    
    if if_exists:
        logger.warning(f"Job hash {result_dict['job_hash']} already exists in {collection_name} in docDB")

        if skip_already_exists:
            logger.warning(f" -- skipped --")
            return {
                "docDB_upload_status": "success (duplicated)",
                "docDB_id": if_exists['_id'],
                "collection_name": collection_name,
                }
        
    # Insert (this will add _id automatically to result_dict)
    response = db.insert_one(result_dict)
    result_dict["_id"] = str(result_dict["_id"])

    if response.acknowledged is False:
        logger.error(f"Failed to insert {result_dict['job_hash']} to {collection_name} in docDB")
        return {"docDB_upload_status": "docDB insertion not acknowledged", "docDB_id": None, "collection_name": None}
    else:
        logger.info(f"Inserted {response.inserted_id} to {collection_name} in docDB")
        return {"docDB_upload_status": "success", "docDB_id": response.inserted_id, "collection_name": collection_name}
