import duckdb
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO or DEBUG as needed
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Log to stdout
    ]
)
logger = logging.getLogger(__name__)

def main():
    with duckdb.connect("databases/law.db") as con:
        con.sql("DROP TABLE IF EXISTS law_url;")
        con.sql("DROP TABLE IF EXISTS law_status;")
        con.sql("DROP TABLE IF EXISTS law;")
        con.sql((
                "CREATE OR REPLACE TABLE law ( "\
                    "id INTEGER PRIMARY KEY," \
                    "type VARCHAR, " \
                    "region VARCHAR, " \
                    "year VARCHAR, " \
                    "title VARCHAR, " \
                    "about VARCHAR, " \
                    "category VARCHAR, "\
                    #"abstract VARCHAR, "\ # ignored as it is too big
                    "detail_url VARCHAR); "
                ))
        con.sql((
                "INSERT INTO law SELECT "\
                    "id, "\
                    "type, "\
                    "region, "\
                    "year, "\
                    "title, "\
                    "about, "\
                    "category, "\
                    "detail_url "\
                    #"abstract "\
                "FROM 'list_law.json';"
                ))    
        con.sql(("CREATE OR REPLACE TABLE law_url " \
                    "(law_id INTEGER REFERENCES law(id), "\
                    "id VARCHAR UNIQUE, "\
                    "download_url VARCHAR); "))
        con.sql(
        ("INSERT INTO law_url SELECT " \
            "id AS law_id, "\
            "CAST(regexp_extract(UNNEST(download_urls), '/Download/([0-9]+)/', 1) AS INTEGER) AS id, "\
            "UNNEST(download_urls) AS download_url FROM 'list_law.json'; ")
        )
        con.sql("CREATE UNIQUE INDEX unique_id_index ON law_url (id);")

        # Create relationship status between laws.
        # The included status is must be a Indonesian passive verb to deduplicate relationship
        # (e.g. this include Diubah, Ditetapkan, Dicabut,
        # but not Mengubah, Menetapkan, Mencabut)
        con.sql(("CREATE OR REPLACE TABLE law_status "\
                    "(affected_law_id INTEGER, "\
                    "affecting_law_id INTEGER, "\
                    # "description VARCHAR, "\
                    "status VARCHAR);"))
        #description VARCHAR
        con.sql(("WITH unnested_law AS " \
                " (SELECT id, UNNEST(statuses) AS statuses FROM 'list_law.json') "\
                "INSERT INTO law_status "\
                "SELECT id AS affected_law_id, "\
                "UNNEST(statuses.associated_uus).id AS affecting_law_id, "\
                "statuses.name AS status FROM unnested_law "\
                "WHERE regexp_matches(status, '^Di'); ")
                )
        #UNNEST(statuses.associated_uus).name AS description
        con.table("law").show()
        con.table("law_url").show()
        con.table("law_status").show()

