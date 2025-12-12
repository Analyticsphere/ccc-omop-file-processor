import json
import random
import uuid
from datetime import date, datetime
from typing import Optional

import core.constants as constants
import core.reporting as reporting
import core.utils as utils
from core.storage_backend import storage


class ReportArtifact:
    def __init__(self, delivery_date: str, artifact_bucket: str, concept_id: Optional[int], name: str, value_as_string: Optional[str], value_as_concept_id: Optional[int], value_as_number: Optional[float]):
        """Initialize ReportArtifact object for creating delivery reports."""
        self.delivery_date = delivery_date
        self.artifact_bucket = artifact_bucket
        self.report_artifact_path = reporting.get_report_tmp_artifacts_path(artifact_bucket, delivery_date)
        self.concept_id = concept_id if concept_id is not None else 0
        self.name = name
        self.value_as_string = value_as_string
        self.value_as_concept_id = value_as_concept_id if value_as_concept_id is not None else 0
        self.value_as_number = value_as_number

    def save_artifact(self) -> None:
        """Save report artifact as Parquet file in temporary report directory."""
        random_id = random.randint(0, 2**31 - 1) # Random, positive, integer within 32 bit signed space
        random_string = str(uuid.uuid4())

        file_path = storage.get_uri(f"{self.report_artifact_path}delivery_report_part_{random_string}{constants.PARQUET}")

        value_as_string_sql = 'NULL' if self.value_as_string is None else f"'{self.value_as_string}'"
        value_as_number_sql = 'NULL' if self.value_as_number is None else f"'{self.value_as_number}'"

        record_statement = f"""
        COPY (
            SELECT
                CAST('{random_id}' AS INT) AS metadata_id,
                TRY_CAST('{self.concept_id}' AS INT) AS metadata_concept_id,
                32880 AS metadata_type_concept_id,
                '{self.name}' AS name,
                {value_as_string_sql} AS value_as_string,
                TRY_CAST('{self.value_as_concept_id}' AS INT) AS value_as_concept_id,
                TRY_CAST({value_as_number_sql} AS FLOAT) AS value_as_number,
                TRY_CAST('{date.today().strftime("%Y-%m-%d")}' AS DATE) AS metadata_date,
                TRY_CAST('{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}' AS DATETIME) AS metadata_datetime
        ) TO '{file_path}' {constants.DUCKDB_FORMAT_STRING}
        """
        utils.execute_duckdb_sql(record_statement, "Unable to save report artifact")

    
    def to_json(self) -> str:
        """
        Returns a JSON string representation of the ReportArtifact's properties.
        """
        return json.dumps(self.__dict__)
