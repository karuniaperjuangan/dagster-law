# pipeline.py
from dagster import job, repository
from jobs.scrape_jobs import scrape_job
from jobs.transform_jobs import transform_job

@job
def scrape_and_transform_pipeline():
    scrape_result = scrape_job()
    transform_result = transform_job()

@repository
def dagster_project_repository():
    return [scrape_and_transform_pipeline]
