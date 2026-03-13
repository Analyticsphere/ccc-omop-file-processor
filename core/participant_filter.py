from typing import Optional, Sequence

import core.constants as constants
import core.helpers.report_artifact as report_artifact
import core.utils as utils
from core.storage_backend import storage


class ParticipantFilter:
    """
    Filters a normalized OMOP parquet file using Connect for Cancer Prevention Study participant eligibility rules.

    This functionality removes a participant's data from an OMOP table if that participant:
    - does not have a "Verified" study status (d_821247024 != 197316935) -OR-
    - has consent withdrawn (d_747006172 = 353358909) -OR-
    - has revoked HIPAA (d_773707518 = 353358909) -OR-
    - has requested data destruction (d_831041022 = 353358909) -OR-
    - does not exist in the Connect database

    An earlier step in the pipeline removes rows without a Connect ID or person_id value.
    """

    VERIFIED_STATUS_CONCEPT_ID = 197316935
    YES_STATUS_CONCEPT_ID = 353358909

    def __init__(self, file_path: str):
        """
        Initialize the table-level Connect participant filter.

        Args:
            file_path: Path to delivered OMOP file.
        """
        self.file_path = file_path
        self.table_name = utils.get_table_name_from_path(file_path).lower()
        self.bucket, self.delivery_date = utils.get_bucket_and_delivery_date_from_path(file_path)
        self.parquet_file_path = utils.get_parquet_artifact_location(file_path)
        # The Connect participant_status file is created earlier in the pipeline and reused here
        self.connect_data_path = utils.get_connect_data_path(self.bucket, self.delivery_date)

    def apply_exclusions(self) -> bool:
        """
        Apply Connect participant exclusions to the normalized parquet file.

        Returns:
            True when filtering was applied.
            False when the table does not contain a person_id column and is skipped.
        """
        if not utils.parquet_file_exists(self.parquet_file_path):
            raise Exception(f"Normalized parquet file not found at {self.parquet_file_path}")

        if not utils.parquet_file_exists(self.connect_data_path):
            raise Exception(f"Connect data parquet not found at {self.connect_data_path}")

        actual_columns = utils.get_columns_from_file(self.parquet_file_path)
        if not self._has_person_id_column(actual_columns):
            utils.logger.info(f"Skipping Connect participant exclusions for {self.table_name}: no person_id column present")
            return False

        filter_sql = ParticipantFilter.generate_filter_sql(
            table_uri=storage.get_uri(self.parquet_file_path),
            connect_data_uri=storage.get_uri(self.connect_data_path)
        )

        utils.execute_duckdb_sql(
            filter_sql,
            f"Unable to apply Connect participant exclusions to {self.parquet_file_path}"
        )

        utils.logger.info(f"Applied Connect participant exclusions to {self.parquet_file_path}")
        return True

    @staticmethod
    def _has_person_id_column(actual_columns: Sequence[str]) -> bool:
        """Return True when the parquet file includes a person_id column."""
        return any(column.lower() == "person_id" for column in actual_columns)

    @staticmethod
    def generate_filter_sql(table_uri: str, connect_data_uri: str) -> str:
        """
        Generate SQL to rewrite a parquet file with only eligible Connect participants.

        The SQL keeps rows when person_id exists in Connect and does not meet any
        exclusion rule. Using a left join against the exclusion set also removes
        person_id values that are missing from Connect entirely.
        """
        return f"""
        COPY (
            WITH known_connect_ids AS (
                SELECT DISTINCT
                    TRY_CAST(Connect_ID AS BIGINT) AS connect_id
                FROM read_parquet('{connect_data_uri}')
                WHERE TRY_CAST(Connect_ID AS BIGINT) IS NOT NULL
            ),
            excluded_connect_ids AS (
                SELECT DISTINCT
                    TRY_CAST(Connect_ID AS BIGINT) AS connect_id
                FROM read_parquet('{connect_data_uri}')
                WHERE TRY_CAST(Connect_ID AS BIGINT) IS NOT NULL
                  AND (
                      TRY_CAST(verified_status_concept_id AS BIGINT) != {ParticipantFilter.VERIFIED_STATUS_CONCEPT_ID}
                      OR TRY_CAST(consent_withdrawn_concept_id AS BIGINT) = {ParticipantFilter.YES_STATUS_CONCEPT_ID}
                      OR TRY_CAST(hipaa_revoked_concept_id AS BIGINT) = {ParticipantFilter.YES_STATUS_CONCEPT_ID}
                      OR TRY_CAST(data_destruction_requested_concept_id AS BIGINT) = {ParticipantFilter.YES_STATUS_CONCEPT_ID}
                  )
            )
            SELECT src.*
            FROM read_parquet('{table_uri}') src
            INNER JOIN known_connect_ids known
                ON TRY_CAST(src.person_id AS BIGINT) = known.connect_id
            LEFT JOIN excluded_connect_ids excluded
                ON TRY_CAST(src.person_id AS BIGINT) = excluded.connect_id
            WHERE excluded.connect_id IS NULL
        ) TO '{table_uri}' {constants.DUCKDB_FORMAT_STRING}
        """.strip()

    @staticmethod
    def create_connect_eligibility_report_artifacts(connect_data_path: str, delivery_bucket: str) -> None:
        """
        Create Connect study report artifacts used to review participant inclusion status. They summarize:
        - status breakdowns for delivery-matched participants
        - delivery-matched participants who meet review/exclusion conditions
        - Connect IDs present only in Connect data
        - person_ids present only in the delivery
        """
        bucket, delivery_date = utils.get_bucket_and_delivery_date_from_path(delivery_bucket)
        person_parquet_path = utils.get_parquet_artifact_location(f"{delivery_bucket}/person.parquet")

        if not utils.parquet_file_exists(person_parquet_path):
            raise Exception(f"Normalized person parquet not found at {person_parquet_path}")

        person_uri = storage.get_uri(person_parquet_path)
        connect_data_uri = storage.get_uri(connect_data_path)

        base_cte = f"""
        WITH person_delivery AS (
            SELECT DISTINCT
                TRY_CAST(person_id AS BIGINT) AS person_id
            FROM read_parquet('{person_uri}')
            WHERE TRY_CAST(person_id AS BIGINT) IS NOT NULL
        ),
        connect_data AS (
            SELECT DISTINCT
                TRY_CAST(Connect_ID AS BIGINT) AS connect_id,
                COALESCE(NULLIF(TRIM(CAST(verified_status AS VARCHAR)), ''), 'UNKNOWN') AS verified_status,
                TRY_CAST(verified_status_concept_id AS BIGINT) AS verified_status_concept_id,
                COALESCE(NULLIF(TRIM(CAST(consent_withdrawn AS VARCHAR)), ''), 'UNKNOWN') AS consent_withdrawn,
                TRY_CAST(consent_withdrawn_concept_id AS BIGINT) AS consent_withdrawn_concept_id,
                COALESCE(NULLIF(TRIM(CAST(hipaa_revoked AS VARCHAR)), ''), 'UNKNOWN') AS hipaa_revoked,
                TRY_CAST(hipaa_revoked_concept_id AS BIGINT) AS hipaa_revoked_concept_id,
                COALESCE(NULLIF(TRIM(CAST(data_destruction_requested AS VARCHAR)), ''), 'UNKNOWN') AS data_destruction_requested,
                TRY_CAST(data_destruction_requested_concept_id AS BIGINT) AS data_destruction_requested_concept_id
            FROM read_parquet('{connect_data_uri}')
            WHERE TRY_CAST(Connect_ID AS BIGINT) IS NOT NULL
        ),
        matched_patients AS (
            SELECT DISTINCT
                p.person_id,
                cd.connect_id,
                cd.verified_status,
                cd.verified_status_concept_id,
                cd.consent_withdrawn,
                cd.consent_withdrawn_concept_id,
                cd.hipaa_revoked,
                cd.hipaa_revoked_concept_id,
                cd.data_destruction_requested,
                cd.data_destruction_requested_concept_id
            FROM person_delivery p
            INNER JOIN connect_data cd
                ON p.person_id = cd.connect_id
        )
        """

        def save_artifact(
            name: str,
            value_as_number: Optional[float] = None,
            value_as_string: Optional[str] = None,
            value_as_concept_id: Optional[int] = None
        ) -> None:
            artifact = report_artifact.ReportArtifact(
                delivery_date=delivery_date,
                artifact_bucket=bucket,
                concept_id=0,
                name=name,
                value_as_string=value_as_string,
                value_as_concept_id=value_as_concept_id,
                value_as_number=value_as_number
            )
            artifact.save_artifact()

        report_configs = [
            (
                "Connect participant breakdown: Study status",
                "verified_status",
                "verified_status_concept_id"
            ),
            (
                "Connect participant breakdown: HIPAA revoked status",
                "hipaa_revoked",
                "hipaa_revoked_concept_id"
            ),
            (
                "Connect participant breakdown: Consent withdrawn status",
                "consent_withdrawn",
                "consent_withdrawn_concept_id"
            ),
            (
                "Connect participant breakdown: Data destruction status",
                "data_destruction_requested",
                "data_destruction_requested_concept_id"
            )
        ]

        for artifact_name, status_column, concept_id_column in report_configs:
            status_count_sql = f"""
            {base_cte}
            SELECT
                {status_column} AS status_value,
                {concept_id_column} AS status_concept_id,
                COUNT(DISTINCT person_id) AS patient_count
            FROM matched_patients
            GROUP BY 1, 2
            ORDER BY 1, 2
            """

            status_counts = utils.execute_duckdb_sql(
                status_count_sql,
                f"Unable to create Connect data report artifacts for {status_column}",
                return_results=True
            )

            for status_value, status_concept_id, patient_count in status_counts:
                save_artifact(
                    name=artifact_name,
                    value_as_string=status_value,
                    value_as_concept_id=status_concept_id,
                    value_as_number=float(patient_count)
                )

        connect_not_in_delivery_sql = f"""
        {base_cte}
        SELECT
            COUNT(*) AS patient_count,
            COALESCE(STRING_AGG(CAST(connect_id AS VARCHAR), '|' ORDER BY connect_id), '') AS patient_ids
        FROM (
            SELECT DISTINCT cd.connect_id
            FROM connect_data cd
            LEFT JOIN person_delivery p
                ON p.person_id = cd.connect_id
            WHERE p.person_id IS NULL
        ) unmatched_connect
        """
        connect_not_in_delivery = utils.execute_duckdb_sql(
            connect_not_in_delivery_sql,
            "Unable to create Connect-vs-delivery reconciliation artifacts for Connect-only patients",
            return_results=True
        )

        connect_only_count, connect_only_ids = connect_not_in_delivery[0] if connect_not_in_delivery else (0, "")
        save_artifact(
            name="Number of Connect patients not in delivery",
            value_as_string=connect_only_ids or "",
            value_as_number=float(connect_only_count)
        )

        delivery_not_in_connect_sql = f"""
        {base_cte}
        SELECT
            COUNT(*) AS patient_count,
            COALESCE(STRING_AGG(CAST(person_id AS VARCHAR), '|' ORDER BY person_id), '') AS patient_ids
        FROM (
            SELECT DISTINCT p.person_id
            FROM person_delivery p
            LEFT JOIN connect_data cd
                ON p.person_id = cd.connect_id
            WHERE cd.connect_id IS NULL
        ) unmatched_delivery
        """
        delivery_not_in_connect = utils.execute_duckdb_sql(
            delivery_not_in_connect_sql,
            "Unable to create Connect-vs-delivery reconciliation artifacts for delivery-only patients",
            return_results=True
        )

        delivery_only_count, delivery_only_ids = delivery_not_in_connect[0] if delivery_not_in_connect else (0, "")
        save_artifact(
            name="Number of delivery patients not in Connect data",
            value_as_string=delivery_only_ids or "",
            value_as_number=float(delivery_only_count)
        )
        save_artifact(
            name="Delivery patient IDs not in Connect data",
            value_as_string=delivery_only_ids or ""
        )
