""" top level run script """

import glob
from tqdm import tqdm
import os
import json

from utils.docDB_io import (
    insert_result_to_docDB_ssh,
    update_job_manager,
    DocumentDbSSHClient,
    credentials,
)
from utils.aws_io import upload_result_to_s3

S3_RESULTS_ROOT = f's3://aind-behavior-data/foraging_nwb_bonsai_processed/v2/'


def run():
    # In /root/capsule/data/aind_analysis_arch_pipeline_results, use glob to find all docDB_job_manager.json files
    all_jobs_jsons = glob.glob('/root/capsule/data/aind_analysis_arch_pipeline_results/**/docDB_job_manager.json', recursive=True)
    
    with DocumentDbSSHClient(credentials=credentials) as doc_db_client:
        for job_json in tqdm(all_jobs_jsons, desc='Processing jobs', total=len(all_jobs_jsons)):
            result_folder = os.path.dirname(job_json)
            job_hash = os.path.basename(os.path.dirname(job_json))
           
            with open(job_json, 'r') as f:
                job_dict = json.load(f)

            collection_name = job_dict['collection_name']
                
            if job_dict['status'] == "success":
                result_json = os.path.join(result_folder, f"docDB_{collection_name}.json")
                with open(result_json, 'r') as f:
                    result_dict = json.load(f)
                                
                # -- Insert result_dict to docDB --
                insert_result_response = insert_result_to_docDB_ssh(
                    result_dict=result_dict,
                    collection_name=collection_name,
                    doc_db_client=doc_db_client,
                )
                job_dict.update(insert_result_response)
                
                # -- Upload results to s3 --
                s3_path = S3_RESULTS_ROOT + job_hash
                upload_result_to_s3(
                    result_folder + '/', 
                    s3_path + '/',
                )
                job_dict.update({
                    "s3_location": s3_path,
                })
            
            # -- Update job_manager --
            response = update_job_manager(
                job_hash=job_hash,
                update_dict=job_dict,
                doc_db_client=doc_db_client,
            )
    

if __name__ == "__main__": run()