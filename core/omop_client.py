import sys

import duckdb  # type: ignore
from google.cloud import storage  # type: ignore
import core.constants as constants
import core.utils as utils


def create_optimized_vocab_file(vocab_version: str, vocab_gcs_bucket: str) -> None:
    vocab_path = f"{vocab_gcs_bucket}/{vocab_version}/"
    optimized_vocab_path = utils.get_optimized_vocab_file_path(vocab_version, vocab_gcs_bucket)

    # Create the optimized vocabulary file if it doesn't exist
    if not utils.parquet_file_exists(optimized_vocab_path):
        # Ensure exisiting vocab file can be read
        if not utils.valid_parquet_file(optimized_vocab_path):
            # Ensure vocabulary version actually exists
            if utils.vocab_gcs_path_exists(vocab_path):
                conn, local_db_file = utils.create_duckdb_connection()

                try:
                    with conn:
                        transform_query = f"""
                        COPY (
                            SELECT DISTINCT
                                c1.concept_id AS concept_id, -- Every concept_id from concept table
                                c1.standard_concept AS concept_id_standard, 
                                c1.domain_id AS concept_id_domain,
                                cr.relationship_id, 
                                cr.concept_id_2 AS target_concept_id, -- targets to concept_id's
                                c2.standard_concept AS target_concept_id_standard, 
                                c2.domain_id AS target_concept_id_domain
                            FROM read_csv('gs://{vocab_path}CONCEPT.csv', delim='\t',strict_mode=False) c1
                            LEFT JOIN read_csv('gs://{vocab_path}CONCEPT_RELATIONSHIP.csv', delim='\t',strict_mode=False) cr on c1.concept_id = cr.concept_id_1
                            LEFT JOIN read_csv('gs://{vocab_path}CONCEPT.csv', delim='\t',strict_mode=False) c2 on cr.concept_id_2 = c2.concept_id
                            WHERE IFNULL(cr.relationship_id, '') 
                                IN ('', {constants.MAPPING_RELATIONSHIPS},{constants.REPLACEMENT_RELATIONSHIPS})
                        ) TO 'gs://{optimized_vocab_path}' {constants.DUCKDB_FORMAT_STRING}
                        """
                        conn.execute(transform_query)
                except Exception as e:
                    utils.logger.error(f"Unable to create optimized vocab file: {e}")
                    sys.exit(1)
                finally:
                    utils.close_duckdb_connection(conn, local_db_file)
            else:
                utils.logger.error(f"Vocabulary GCS bucket {vocab_path} not found")
                sys.exit(1)
    else:
        utils.logger.info(f"Optimized vocabulary already exists")

def create_missing_tables(project_id: str, dataset_id: str, omop_version: str) -> None:
    print()