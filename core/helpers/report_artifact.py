import core.constants as constants
import core.utils as utils
from typing import Optional
import random
import uuid
from datetime import datetime, date
import sys
import json

class ReportArtifact:
    def __init__(self, delivery_date: str, gcs_path: str, concept_id: Optional[int], name: str, value_as_string: Optional[str], value_as_concept_id: Optional[int], value_as_number: Optional[float]):
        self.delivery_date = delivery_date
        self.gcs_path = gcs_path
        self.report_artifact_path = f"gs://{gcs_path}/{delivery_date}/{constants.ArtifactPaths.REPORT_TMP.value}"
        self.concept_id = concept_id if concept_id is not None else 0
        self.name = name
        self.value_as_string = value_as_string
        self.value_as_concept_id = value_as_concept_id if value_as_concept_id is not None else 0
        self.value_as_number = value_as_number

    def save_artifact(self) -> None:
        random_id = random.randint(0, 2**31 - 1) # Random, positive, integer within 32 bit signed space
        random_string = str(uuid.uuid4())

        file_path = f"{self.report_artifact_path}delivery_report_part_{random_string}{constants.PARQUET}"

        conn, local_db_file = utils.create_duckdb_connection()    

        try:
            with conn:
                record_statement = f"""
                COPY (
                    SELECT
                        CAST('{random_id}' AS INT) AS metadata_id,
                        TRY_CAST('{self.concept_id}' AS INT) AS metadata_concept_id,
                        32880 AS metadata_type_concept_id,
                        '{self.name}' AS name,
                        CAST('{self.value_as_string}' AS STRING) AS value_as_string,
                        TRY_CAST('{self.value_as_concept_id}' AS INT) AS value_as_concept_id,
                        TRY_CAST('{self.value_as_number}' AS FLOAT) AS value_as_number,
                        TRY_CAST('{date.today().strftime("%Y-%m-%d")}' AS DATE) AS metadata_date,
                        TRY_CAST('{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}' AS DATETIME) AS metadata_datetime
                ) TO '{file_path}' {constants.DUCKDB_FORMAT_STRING}
                """
                conn.execute(record_statement)
                utils.logger.info(f"Saved delivery report record to {file_path}")
        except Exception as e:
            utils.logger.error(f"Unable to save : {e}")
            sys.exit(1)
        finally:
            utils.close_duckdb_connection(conn, local_db_file)
    
    def to_json(self) -> str:
        """
        Returns a JSON string representation of the ReportArtifact's properties.
        """
        return json.dumps(self.__dict__)
    