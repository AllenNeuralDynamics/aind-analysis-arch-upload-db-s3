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
from utils.aws_io import upload_result_to_s3, S3_RESULTS_ROOT
from utils.reformat import split_nwb_name

# Get script directory
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(f'{SCRIPT_DIR}/../results/upload.log'),
                              logging.StreamHandler()])


def reformat_result_dict_for_docDB(result_dict, status):
    """To match the latest docDB format for MLE fitting.
    
    See https://github.com/AllenNeuralDynamics/aind-analysis-arch-result-access/blob/d8f797680f7dce316f0025b1efc976fb3bf78af3/src/aind_analysis_arch_result_access/patch/patch_20250213_migrate_database.py
    """

    subject_id, session_date, _ = split_nwb_name(result_dict["nwb_name"])

    # -- Basic fields --
    dict_to_docDB = {"nwb_name": result_dict["nwb_name"]}
    analysis_spec = result_dict["analysis_spec"].copy()
    # remove a field added during analysis wrapper but not initially in analysis_spec (that generates the hash)
    analysis_spec["analysis_args"]["fit_kwargs"]["DE_kwargs"].pop("workers", None)

    dict_to_docDB.update(
        {
            "_id": result_dict["job_hash"],
            "status": status,
            "s3_location": None,  # Update later if status = success
            "subject_id": subject_id,
            "session_date": session_date,
            "analysis_spec": analysis_spec,
        }
    )   

    # If status is not success, return without other fields
    if status != "success":
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
def upload_one_job(job_json, skip_already_exists=False):
    result_folder = os.path.dirname(job_json)

    try:
        # Load job_json and result_json
        with open(job_json, 'r') as f:
            job_dict = json.load(f)
            
        result_json = os.path.join(result_folder, f"docDB_{job_dict['collection_name']}.json")

        with open(result_json, 'r') as f:
            result_dict = json.load(f)

        job_hash = result_dict['job_hash']

        # Do some ad-hoc formatting to the result_dict for backward compatibility
        dict_to_docDB = reformat_result_dict_for_docDB(
            result_dict=result_dict, status=job_dict["status"]
        )

        # Insert result_dict to DocumentDB
        upsert_result_to_docDB = upload_docDB_record_to_prod(
            dict_to_docDB=dict_to_docDB,
            skip_already_exists=skip_already_exists,
        )

        # If job is successful AND upload docDB is success, upload the result to S3
        if job_dict["status"] == "success" and upsert_result_to_docDB == "upload docDB success":
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
