# scrape_job.py
import os
import asyncio
from dagster import job, op, Output, ResourceDefinition
from task.scrape import main as scrape_main

TEMP_DIR = 'temp_laws'
os.makedirs(TEMP_DIR, exist_ok=True)

@op
def run_scraping():
    """Wrapper for running the main scraping coroutine from scrape.py."""
    asyncio.run(scrape_main())
    return Output(None)

@job
def scrape_job():
    run_scraping()
