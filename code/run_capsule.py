import glob
import os
import json
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count

from utils.docDB_io import (
    insert_result_to_docDB_ssh,
    update_job_manager,
    DocumentDbSSHClient,
    credentials,
)
from utils.aws_io import upload_result_to_s3

# Get script directory
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(f'{SCRIPT_DIR}/../results/upload.log'),
                              logging.StreamHandler()])

S3_RESULTS_ROOT = f's3://aind-behavior-data/foraging_nwb_bonsai_processed/v2/'

# Helper function to process each job
def process_job(job_json, doc_db_client):
    result_folder = os.path.dirname(job_json)
    job_hash = os.path.basename(os.path.dirname(job_json))
    
    try:
        with open(job_json, 'r') as f:
            job_dict = json.load(f)

        collection_name = job_dict['collection_name']
        
        if job_dict['status'] == "success":
            result_json = os.path.join(result_folder, f"docDB_{collection_name}.json")
            with open(result_json, 'r') as f:
                result_dict = json.load(f)

            # Insert result_dict to DocumentDB
            insert_result_response = insert_result_to_docDB_ssh(
                result_dict=result_dict,
                collection_name=collection_name,
                doc_db_client=doc_db_client,
                skip_already_exists=True,
            )
            job_dict.update(insert_result_response)

            # Upload results to S3
            s3_path = S3_RESULTS_ROOT + job_hash
            upload_result_to_s3(result_folder + '/', s3_path + '/')
            job_dict.update({
                "s3_location": s3_path,
            })

        # Update job manager
        update_job_manager(
            job_hash=job_hash,
            update_dict=job_dict,
            doc_db_client=doc_db_client,
        )
        logging.info(f"Successfully processed job: {job_hash}")
    
    except Exception as e:
        logging.error(f"Error processing job {job_hash}: {e}")

def run():
    all_jobs_jsons = glob.glob(f'{SCRIPT_DIR}/../data/**/docDB_job_manager.json', recursive=True)

    if len(all_jobs_jsons) == 0:
        logging.warning("No jobs found to process.")
        return

    # Use a thread pool to process jobs in parallel
    num_threads = 100  # since it is not a very CPU-heavy task
    with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            list(tqdm(executor.map(lambda job_json: process_job(job_json, doc_db_client), all_jobs_jsons), total=len(all_jobs_jsons), desc='Processing jobs'))


if __name__ == "__main__":
    logging.info("Job processing script started.")
    try:
        run()
    except Exception as e:
        logging.critical(f"Critical error during script execution: {e}")
    logging.info("Job processing script finished.")
