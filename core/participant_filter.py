from typing import Optional, Sequence

import core.constants as constants
import core.helpers.report_artifact as report_artifact
import core.utils as utils
from core.storage_backend import storage


class ParticipantFilter:
    """
    Filters a normalized OMOP parquet file using Connect for Cancer Prevention Study participant eligibility rules.

    This functionality removes a participant's data from an OMOP table if that participant:
    - does not have a valid person_id value (NULL, non-numeric, or -1) -OR-
    - does not have a "Verified" study status (d_821247024 != 197316935) -OR-
    - has consent withdrawn (d_747006172 = 353358909) -OR-
    - has revoked HIPAA (d_773707518 = 353358909) -OR-
    - has requested data destruction (d_831041022 = 353358909) -OR-
    - does not exist in the Connect database
    """

    VERIFIED_STATUS_CONCEPT_ID = 197316935
    YES_STATUS_CONCEPT_ID = 353358909
    UNKNOWN_IDENTIFIER_BUCKET = "unknown_identifier"
    IDENTIFIER_NOT_IN_CONNECT_BUCKET = "identifier_not_in_connect"
    CONNECT_EXCLUSION_BUCKET = "connect_exclusion_rules"

    def __init__(self, file_path: str, omop_version: str):
        """
        Initialize the table-level Connect participant filter.

        Args:
            file_path: Path to delivered OMOP file.
            omop_version: OMOP CDM version used to look up table concept IDs.
        """
        self.file_path = file_path
        self.omop_version = omop_version
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

        table_uri = storage.get_uri(self.parquet_file_path)
        row_removal_counts = self._get_row_removal_counts(
            table_uri=table_uri,
            connect_data_uri=storage.get_uri(self.connect_data_path)
        )
        self._create_row_removal_artifacts(row_removal_counts)

        filter_sql = ParticipantFilter.generate_filter_sql(
            table_uri=table_uri,
            connect_data_uri=storage.get_uri(self.connect_data_path)
        )

        utils.execute_duckdb_sql(
            filter_sql,
            f"Unable to apply Connect participant exclusions to {self.parquet_file_path}"
        )

        utils.logger.info(f"Applied Connect participant exclusions to {self.parquet_file_path}")
        return True

    def _get_row_removal_counts(self, table_uri: str, connect_data_uri: str) -> dict[str, int]:
        """Return mutually exclusive row-removal counts for the current OMOP table."""
        count_sql = ParticipantFilter.generate_row_removal_count_sql(
            table_uri=table_uri,
            connect_data_uri=connect_data_uri
        )
        result = utils.execute_duckdb_sql(
            count_sql,
            f"Unable to count rows removed by participant filter in {self.parquet_file_path}",
            return_results=True
        )
        unknown_count, missing_from_connect_count, excluded_count = result[0] if result else (0, 0, 0)

        return {
            ParticipantFilter.UNKNOWN_IDENTIFIER_BUCKET: int(unknown_count),
            ParticipantFilter.IDENTIFIER_NOT_IN_CONNECT_BUCKET: int(missing_from_connect_count),
            ParticipantFilter.CONNECT_EXCLUSION_BUCKET: int(excluded_count)
        }

    def _create_row_removal_artifacts(self, row_removal_counts: dict[str, int]) -> None:
        """Save per-table row-removal artifacts for the participant filter's three buckets."""
        schema = utils.get_cdm_schema(self.omop_version)
        table_concept_id = int(schema[self.table_name]['concept_id'])

        unknown_count = row_removal_counts[ParticipantFilter.UNKNOWN_IDENTIFIER_BUCKET]

        artifact_names_and_counts = [
            (f"Number of rows removed due to missing person_id values: {self.table_name}", unknown_count),
            (f"Number of rows removed due to identifier not in Connect database: {self.table_name}",
             row_removal_counts[ParticipantFilter.IDENTIFIER_NOT_IN_CONNECT_BUCKET]),
            (f"Number of rows removed due to Connect exclusion rules: {self.table_name}",
             row_removal_counts[ParticipantFilter.CONNECT_EXCLUSION_BUCKET]),
        ]

        if self.table_name == "person":
            artifact_names_and_counts.insert(0, ("Number of persons with missing person_id", unknown_count))

        for artifact_name, artifact_count in artifact_names_and_counts:
            artifact = report_artifact.ReportArtifact(
                delivery_date=self.delivery_date,
                artifact_bucket=self.bucket,
                concept_id=table_concept_id,
                name=artifact_name,
                value_as_string=None,
                value_as_concept_id=None,
                value_as_number=artifact_count
            )
            artifact.save_artifact()

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
            WITH filtered_source AS (
                SELECT *
                FROM read_parquet('{table_uri}')
                WHERE TRY_CAST(person_id AS BIGINT) IS NOT NULL
                  AND TRY_CAST(person_id AS BIGINT) != -1
            ),
            known_connect_ids AS (
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
            FROM filtered_source src
            INNER JOIN known_connect_ids known
                ON TRY_CAST(src.person_id AS BIGINT) = known.connect_id
            LEFT JOIN excluded_connect_ids excluded
                ON TRY_CAST(src.person_id AS BIGINT) = excluded.connect_id
            WHERE excluded.connect_id IS NULL
        ) TO '{table_uri}' {constants.DUCKDB_FORMAT_STRING}
        """.strip()

    @staticmethod
    def generate_row_removal_count_sql(table_uri: str, connect_data_uri: str) -> str:
        """
        Generate SQL to count rows removed by the participant filter's mutually exclusive buckets.

        Bucket precedence:
        1. unknown_identifier
        2. identifier_not_in_connect
        3. connect_exclusion_rules
        """
        return f"""
        WITH source_rows AS (
            SELECT
                TRY_CAST(person_id AS BIGINT) AS normalized_person_id
            FROM read_parquet('{table_uri}')
        ),
        known_connect_ids AS (
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
        ),
        classified_rows AS (
            SELECT
                CASE
                    WHEN src.normalized_person_id IS NULL OR src.normalized_person_id = -1
                        THEN '{ParticipantFilter.UNKNOWN_IDENTIFIER_BUCKET}'
                    WHEN known.connect_id IS NULL
                        THEN '{ParticipantFilter.IDENTIFIER_NOT_IN_CONNECT_BUCKET}'
                    WHEN excluded.connect_id IS NOT NULL
                        THEN '{ParticipantFilter.CONNECT_EXCLUSION_BUCKET}'
                    ELSE 'retained'
                END AS removal_bucket
            FROM source_rows src
            LEFT JOIN known_connect_ids known
                ON src.normalized_person_id = known.connect_id
            LEFT JOIN excluded_connect_ids excluded
                ON src.normalized_person_id = excluded.connect_id
        )
        SELECT
            COALESCE(SUM(CASE WHEN removal_bucket = '{ParticipantFilter.UNKNOWN_IDENTIFIER_BUCKET}' THEN 1 ELSE 0 END), 0)
                AS unknown_identifier_count,
            COALESCE(SUM(CASE WHEN removal_bucket = '{ParticipantFilter.IDENTIFIER_NOT_IN_CONNECT_BUCKET}' THEN 1 ELSE 0 END), 0)
                AS identifier_not_in_connect_count,
            COALESCE(SUM(CASE WHEN removal_bucket = '{ParticipantFilter.CONNECT_EXCLUSION_BUCKET}' THEN 1 ELSE 0 END), 0)
                AS connect_exclusion_rules_count
        FROM classified_rows
        """.strip()

    @staticmethod
    def generate_delivery_not_in_connect_sql(
        table_uri: str,
        connect_data_uri: str,
        invalid_rows_uri: str = "",
        has_connect_id_column: bool = False
    ) -> str:
        """
        Generate SQL to find person_ids from a table that are not in the Connect database.

        Combines two sources of unmatched IDs:
        1. Numeric person_ids in the converted parquet that do not match any Connect_ID
        2. Non-numeric connect_id values in the invalid_rows parquet (when present)

        Excludes -1 person_id values, which represent missing identifiers handled by a
        separate reporting bucket.
        """
        non_numeric_cte = ""
        non_numeric_union = ""

        if invalid_rows_uri:
            id_column = "connect_id" if has_connect_id_column else "person_id"
            non_numeric_cte = f""",
            non_numeric_ids AS (
                SELECT DISTINCT CAST({id_column} AS VARCHAR) AS unmatched_id
                FROM read_parquet('{invalid_rows_uri}')
                WHERE {id_column} IS NOT NULL
                  AND TRIM(CAST({id_column} AS VARCHAR)) != ''
                  AND TRY_CAST({id_column} AS BIGINT) IS NULL
            )"""
            non_numeric_union = """
                UNION
                SELECT unmatched_id FROM non_numeric_ids"""

        return f"""
        WITH table_ids AS (
            SELECT DISTINCT
                TRY_CAST(person_id AS BIGINT) AS person_id
            FROM read_parquet('{table_uri}')
            WHERE TRY_CAST(person_id AS BIGINT) IS NOT NULL
              AND TRY_CAST(person_id AS BIGINT) != -1
        ),
        connect_ids AS (
            SELECT DISTINCT
                TRY_CAST(Connect_ID AS BIGINT) AS connect_id
            FROM read_parquet('{connect_data_uri}')
            WHERE TRY_CAST(Connect_ID AS BIGINT) IS NOT NULL
        ),
        numeric_not_in_connect AS (
            SELECT CAST(t.person_id AS VARCHAR) AS unmatched_id
            FROM table_ids t
            LEFT JOIN connect_ids cd
                ON t.person_id = cd.connect_id
            WHERE cd.connect_id IS NULL
        ){non_numeric_cte},
        all_unmatched AS (
            SELECT unmatched_id FROM numeric_not_in_connect{non_numeric_union}
        )
        SELECT
            COUNT(*) AS patient_count,
            COALESCE(STRING_AGG(unmatched_id, '|' ORDER BY unmatched_id), '') AS patient_ids
        FROM all_unmatched
        """.strip()

    @staticmethod
    def create_connect_eligibility_report_artifacts(connect_data_path: str, delivery_bucket: str) -> None:
        """
        Create Connect study report artifacts used to review participant inclusion status. They summarize:
        - status breakdowns for delivery-matched participants, with IDs only for exclusion-driving statuses
        - delivery-matched participants who meet review/exclusion conditions
        - eligible Connect IDs present only in Connect data
        - person_ids present only in the delivery (per table)
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

        def should_save_status_ids(status_column: str, status_concept_id: Optional[int]) -> bool:
            """Return True when a status bucket represents an exclusion-driving condition."""
            if status_column == "verified_status":
                return status_concept_id is not None and status_concept_id != ParticipantFilter.VERIFIED_STATUS_CONCEPT_ID

            return status_concept_id == ParticipantFilter.YES_STATUS_CONCEPT_ID

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
                COUNT(DISTINCT person_id) AS patient_count,
                COALESCE(STRING_AGG(CAST(connect_id AS VARCHAR), '|' ORDER BY connect_id), '') AS patient_ids
            FROM matched_patients
            GROUP BY 1, 2
            ORDER BY 1, 2
            """

            status_counts = utils.execute_duckdb_sql(
                status_count_sql,
                f"Unable to create Connect data report artifacts for {status_column}",
                return_results=True
            )

            for status_value, status_concept_id, patient_count, patient_ids in status_counts:
                save_artifact(
                    name=f"{artifact_name} ({status_value})",
                    value_as_string=patient_ids if should_save_status_ids(status_column, status_concept_id) else "",
                    value_as_concept_id=status_concept_id,
                    value_as_number=float(patient_count)
                )

        connect_not_in_delivery_sql = f"""
        {base_cte}
        -- Only count Connect participants who would survive the participant filter;
        -- excluded Connect records are not actionable "missing from delivery" patients.
        SELECT
            COUNT(*) AS patient_count,
            COALESCE(STRING_AGG(CAST(connect_id AS VARCHAR), '|' ORDER BY connect_id), '') AS patient_ids
        FROM (
            SELECT DISTINCT eligible_connect.connect_id
            FROM (
                SELECT DISTINCT cd.connect_id
                FROM connect_data cd
                WHERE COALESCE(cd.verified_status_concept_id, 0) = {ParticipantFilter.VERIFIED_STATUS_CONCEPT_ID}
                  AND COALESCE(cd.consent_withdrawn_concept_id, 0) != {ParticipantFilter.YES_STATUS_CONCEPT_ID}
                  AND COALESCE(cd.hipaa_revoked_concept_id, 0) != {ParticipantFilter.YES_STATUS_CONCEPT_ID}
                  AND COALESCE(cd.data_destruction_requested_concept_id, 0) != {ParticipantFilter.YES_STATUS_CONCEPT_ID}
            ) eligible_connect
            LEFT JOIN person_delivery p
                ON p.person_id = eligible_connect.connect_id
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
            name="Number of eligible Connect patients not in delivery",
            value_as_string=connect_only_ids or "",
            value_as_number=float(connect_only_count)
        )

        # Per-table "delivery not in Connect" artifacts
        converted_files_prefix = f"{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}"
        table_filenames = utils.list_files(bucket, converted_files_prefix, constants.PARQUET)

        for table_filename in sorted(table_filenames):
            table_name = utils.get_table_name_from_path(table_filename)
            table_path = f"{bucket}/{converted_files_prefix}{table_filename}"
            table_uri = storage.get_uri(table_path)

            actual_columns = utils.get_columns_from_file(table_path)
            if not ParticipantFilter._has_person_id_column(actual_columns):
                continue

            # Check for non-numeric identifiers in invalid rows
            invalid_rows_path = utils.get_invalid_rows_path_from_path(f"{bucket}/{delivery_date}/{table_name}.parquet")
            invalid_rows_uri = ""
            has_connect_id_column = False
            if utils.parquet_file_exists(invalid_rows_path):
                invalid_rows_uri = storage.get_uri(invalid_rows_path)
                invalid_rows_columns = utils.get_columns_from_file(invalid_rows_path)
                has_connect_id_column = any(
                    'connectid' in col.lower() or 'connect_id' in col.lower()
                    for col in invalid_rows_columns
                )

            delivery_not_in_connect_sql = ParticipantFilter.generate_delivery_not_in_connect_sql(
                table_uri=table_uri,
                connect_data_uri=connect_data_uri,
                invalid_rows_uri=invalid_rows_uri,
                has_connect_id_column=has_connect_id_column
            )

            delivery_not_in_connect = utils.execute_duckdb_sql(
                delivery_not_in_connect_sql,
                f"Unable to create delivery-not-in-Connect artifacts for {table_name}",
                return_results=True
            )

            delivery_only_count, delivery_only_ids = delivery_not_in_connect[0] if delivery_not_in_connect else (0, "")
            save_artifact(
                name=f"Delivered Connect ID values not found in Connect database: {table_name}",
                value_as_string=delivery_only_ids or "",
                value_as_number=float(delivery_only_count)
            )
