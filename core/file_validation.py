from typing import Any, Optional

import core.helpers.report_artifact as report_artifact
import core.utils as utils


class FileValidator:
    """
    Validates OMOP CDM files against schema requirements.

    Performs validation of:
    - Table name validity
    - Column presence and correctness
    - Schema compliance

    Creates report artifacts documenting validation results.
    """

    def __init__(self, file_path: str, omop_version: str, delivery_date: str, storage_path: str):
        """
        Initialize validator for a specific file.

        Args:
            file_path: Path to file being validated
            omop_version: OMOP CDM version (e.g., "5.4")
            delivery_date: Delivery date for reporting
            storage_path: Storage bucket/path for report artifacts
        """
        self.file_path = file_path
        self.omop_version = omop_version
        self.delivery_date = delivery_date
        self.storage_path = storage_path

        # Derived attributes computed once
        self.table_name = utils.get_table_name_from_path(file_path)
        self.bucket_name, _ = utils.get_bucket_and_delivery_date_from_path(file_path)

        # Loaded on demand
        self._cdm_schema: Optional[dict[Any, Any]] = None
        self._table_schema: Optional[dict[Any, Any]] = None

    def validate(self) -> None:
        """
        Execute complete file validation.

        Validates table name and columns, creating report artifacts
        for each validation result.

        Raises:
            Exception: If validation encounters errors
        """
        utils.logger.info(f"Validating {self.file_path} against OMOP v{self.omop_version}")

        try:
            valid_table_name = self.validate_table_name()
            if valid_table_name:
                self.validate_columns()
        except Exception as e:
            raise Exception(f"Error validating file {self.file_path}: {str(e)}") from e

    def validate_table_name(self) -> bool:
        """
        Validate that the table name matches an OMOP CDM table.

        Creates a report artifact documenting whether the table name is valid.

        Returns:
            True if table name is valid, False otherwise

        Raises:
            Exception: If validation encounters errors
        """
        try:
            schema = self._get_cdm_schema()
            valid_table_names = schema.keys()
            is_valid = self.table_name in valid_table_names

            if is_valid:
                self._create_report_artifact(
                    concept_id=schema[self.table_name]['concept_id'],
                    name=f"Valid table name: {self.table_name}",
                    value="valid table name"
                )
            else:
                self._create_report_artifact(
                    concept_id=None,
                    name=f"Invalid table name: {self.table_name}",
                    value="invalid table name"
                )

            return is_valid

        except Exception as e:
            raise Exception(f"Error validating table name for {self.file_path}: {str(e)}") from e

    def validate_columns(self) -> None:
        """
        Validate columns against OMOP CDM schema.

        Checks that:
        - All parquet columns are valid schema columns
        - All required schema columns are present

        Creates report artifacts for valid columns, invalid columns,
        and missing columns.

        Raises:
            Exception: If validation encounters errors
        """
        try:
            schema = self._get_table_schema()
            parquet_columns = self._get_parquet_columns()
            schema_columns = set(schema[self.table_name]['columns'].keys())

            # Categorize columns
            valid_columns = parquet_columns & schema_columns
            invalid_columns = parquet_columns - schema_columns
            missing_columns = schema_columns - parquet_columns

            # Report valid columns
            for column in valid_columns:
                concept_id = schema[self.table_name]['columns'][column]['concept_id']
                self._create_report_artifact(
                    concept_id=concept_id,
                    name=f"Valid column name: {self.table_name}.{column}",
                    value="valid column name"
                )

            # Report invalid columns (in parquet but not in schema)
            for column in invalid_columns:
                self._create_report_artifact(
                    concept_id=None,
                    name=f"Invalid column name: {self.table_name}.{column}",
                    value="invalid column name"
                )

            # Report missing columns (in schema but not in parquet)
            for column in missing_columns:
                concept_id = schema[self.table_name]['columns'][column]['concept_id']
                self._create_report_artifact(
                    concept_id=concept_id,
                    name=f"Missing column: {self.table_name}.{column}",
                    value="missing column"
                )

        except Exception as e:
            raise Exception(f"Error validating columns for {self.file_path}: {str(e)}") from e

    def _get_cdm_schema(self) -> dict[Any, Any]:
        """Get CDM schema, caching for reuse."""
        if self._cdm_schema is None:
            self._cdm_schema = utils.get_cdm_schema(cdm_version=self.omop_version)
        return self._cdm_schema

    def _get_table_schema(self) -> dict[Any, Any]:
        """Get table-specific schema, caching for reuse."""
        if self._table_schema is None:
            self._table_schema = utils.get_table_schema(
                table_name=self.table_name,
                cdm_version=self.omop_version
            )
        return self._table_schema

    def _get_parquet_columns(self) -> set:
        """Get column names from parquet file."""
        parquet_path = utils.get_parquet_artifact_location(self.file_path)
        columns = utils.get_columns_from_file(parquet_path)
        return set(columns)

    def _create_report_artifact(self, concept_id: int | None, name: str, value: str) -> None:
        """
        Create and save a validation report artifact.

        Args:
            concept_id: OMOP concept ID (None for invalid items)
            name: Artifact name describing the validation result
            value: String value describing the result
        """
        artifact = report_artifact.ReportArtifact(
            concept_id=concept_id,
            delivery_date=self.delivery_date,
            artifact_bucket=self.bucket_name,
            name=name,
            value_as_concept_id=None,
            value_as_number=None,
            value_as_string=value
        )
        artifact.save_artifact()
