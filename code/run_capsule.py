import glob
import os
import json
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count

from utils.docDB_io import (
    upload_docDB_record_to_prod,
)
from utils.aws_io import upload_result_to_s3

# Get script directory
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(f'{SCRIPT_DIR}/../results/upload.log'),
                              logging.StreamHandler()])

# Helper function to process each job
def upload_one_job(job_json):
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
            upsert_result_to_docDB = upload_docDB_record_to_prod(
                result_dict=result_dict,
                skip_already_exists=True,
            )
            job_dict.update(upsert_result_to_docDB)

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
    all_jobs_jsons = glob.glob(f'{SCRIPT_DIR}/../data/**/docDB_job_manager.json', recursive=True)[:1]

    if len(all_jobs_jsons) == 0:
        logging.warning("No jobs found to process.")
        return

    # Use a thread pool to process jobs in parallel
    num_threads = 100  # since it is not a very CPU-heavy task
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        list(tqdm(executor.map(lambda job_json: upload_one_job(job_json), all_jobs_jsons), total=len(all_jobs_jsons), desc='Uploading jobs'))


if __name__ == "__main__":
    logging.info("Job processing script started.")
    try:
        run()
    except Exception as e:
        logging.critical(f"Critical error during script execution: {e}")
    logging.info("Job processing script finished.")
