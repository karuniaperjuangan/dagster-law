# transform_job.py
import duckdb
from dagster import job, op, ResourceDefinition
from task.transform import main as transform_main
DB_PATH = "databases/law.db"

@op
def transform_data():
    transform_main()

@job
def transform_job():
    transform_data()
