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
from utils.aws_io import upload_directory_to_s3, S3_RESULTS_ROOT
from utils.reformat import split_nwb_name

# Get script directory
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(f'{SCRIPT_DIR}/../results/upload.log'),
                              logging.StreamHandler()])


def prepare_dict_for_docDB(job_dict, result_dict=None):
    """To match the latest docDB format for MLE fitting.
    
    See https://github.com/AllenNeuralDynamics/aind-analysis-arch-result-access/blob/d8f797680f7dce316f0025b1efc976fb3bf78af3/src/aind_analysis_arch_result_access/patch/patch_20250213_migrate_database.py
    """
    status = job_dict["status"]
    subject_id, session_date, _ = split_nwb_name(job_dict["nwb_name"])

    # -- Basic fields --
    dict_to_docDB = {"nwb_name": job_dict["nwb_name"]}
    analysis_spec = job_dict["analysis_spec"].copy()
    # remove a field added during analysis wrapper but not initially in analysis_spec (that generates the hash)
    analysis_spec["analysis_args"]["fit_kwargs"]["DE_kwargs"].pop("workers", None)

    dict_to_docDB.update(
        {
            "_id": job_dict["job_hash"],
            "status": status,
            "s3_location": None,  # Update later if status = success
            "subject_id": subject_id,
            "session_date": session_date,
            "analysis_spec": analysis_spec,
        }
    )   

    # If status is not success, return without other fields
    if result_dict is None:
        return dict_to_docDB

    # -- Otherwise, add more fields for results --
    more_fields_to_copy = [
        "analysis_datetime",
        "analysis_libs_to_track_ver",
        "analysis_time_spent_in_sec",
    ]

    # Simplify analysis_results (removing latent variables etc)
    analysis_results = result_dict["analysis_results"].copy()
    analysis_results.pop("population", None)
    analysis_results.pop("population_energies", None)
    analysis_results.pop("fitted_latent_variables", None)
    analysis_results["cross_validation"].pop("fitting_results_each_fold", None)
    analysis_results["fit_settings"].pop("fit_reward_history", None)
    analysis_results["fit_settings"].pop("fit_choice_history", None)

    # Fields that need some modifications or added
    dict_to_docDB.update(
        {
            **{field: result_dict[field] for field in more_fields_to_copy},
            "s3_location": f"{S3_RESULTS_ROOT}/{result_dict['job_hash']}",
            "analysis_results": analysis_results,
        }
    )  
    return dict_to_docDB

# Helper function to process each job
def upload_one_job(job_json, skip_already_exists=True):
    result_folder = os.path.dirname(job_json)

    try:
        # Load job_json and result_json
        with open(job_json, 'r') as f:
            job_dict = json.load(f)
            
        job_hash, status = job_dict["job_hash"], job_dict["status"]
        
        if status == "success":
            result_json = os.path.join(result_folder, f"docDB_{job_dict['collection_name']}.json")
            with open(result_json, 'r') as f:
                result_dict = json.load(f)
        else:
            result_dict = None

        # Do some ad-hoc formatting to the result_dict for backward compatibility
        dict_to_docDB = prepare_dict_for_docDB(
            job_dict=job_dict,
            result_dict=result_dict,
        )

        # Insert result_dict to DocumentDB
        upsert_result_to_docDB = upload_docDB_record_to_prod(
            dict_to_docDB=dict_to_docDB,
            skip_already_exists=skip_already_exists,
        )

        # If job is successful AND upload docDB is success, upload the result to S3
        if status == "success" and upsert_result_to_docDB == "upload docDB success":
            # Upload the whole folder to S3 using the new way (boto3)
            upload_directory_to_s3(
                local_directory=result_folder,
                s3_bucket_name=S3_RESULTS_ROOT,
                s3_relative_path=job_hash,
            )
        else:
            logging.info("-- Skipped uploading s3!")

        logging.info(f"Successfully processed job: {job_hash}")

    except Exception as e:
        logging.exception(f"Error processing job {job_hash}: {e}")

def run():
    all_jobs_jsons = glob.glob(f'{SCRIPT_DIR}/../data/**/docDB_job_manager.json', recursive=True)
    
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
