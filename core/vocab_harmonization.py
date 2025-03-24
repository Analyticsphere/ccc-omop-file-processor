import core.constants as constants
import core.utils as utils
import uuid

class VocabHarmonizer:
    """
    A class for harmonizing parquet files according to specified vocabularies and mappings.
    Handles the entire process from reading parquet files to generating SQL and saving the harmonized output.
    """
    
    def __init__(self, gcs_file_path: str, cdm_version: str, site: str, vocab_version: str, vocab_gcs_bucket: str):
        """
        Initialize a VocabHarmonizer with common parameters needed across all operations.
        """
        self.gcs_file_path = gcs_file_path
        self.cdm_version = cdm_version
        self.site = site
        self.vocab_version = vocab_version
        self.vocab_gcs_bucket = vocab_gcs_bucket
        self.table_name = utils.get_table_name_from_gcs_path(gcs_file_path)
        self.bucket = utils.get_bucket_and_delivery_date_from_gcs_path(gcs_file_path)[0]
        self.delivery_date = utils.get_bucket_and_delivery_date_from_gcs_path(gcs_file_path)[1]
        self.parquet_path = utils.get_parquet_artifact_location(gcs_file_path)
        self.case_when_target_table = f"""
            CASE 
                WHEN tbl.target_domain = 'Visit' THEN 'visit_occurrence'
                WHEN tbl.target_domain = 'Condition' THEN 'condition_occurrence'
                WHEN tbl.target_domain = 'Drug' THEN 'drug_exposure'
                WHEN tbl.target_domain = 'Procedure' THEN 'procedure_occurrence'
                WHEN tbl.target_domain = 'Device' THEN 'device_exposure'
                WHEN tbl.target_domain = 'Measurement' THEN 'measurement'
                WHEN tbl.target_domain = 'Observation' THEN 'observation'
                WHEN tbl.target_domain = 'Note' THEN 'note'
                WHEN tbl.target_domain = 'Specimen' THEN 'specimen'
            ELSE '{self.table_name}' END AS target_table
        """
    
    def harmonize_parquet_file(self) -> None:
        """
        Harmonize a parquet file by applying defined harmonization steps and saving the result.
        """

        # List order is very important here!
        harmonization_steps: list = [constants.SOURCE_TARGET]

        for step in harmonization_steps:
            self.perform_harmonization(step)

        testing_sql = self.get_source_target_mapping_sql()

        try:
            conn, local_db_file = utils.create_duckdb_connection()

            with conn:
                resave_statement = f"""
                    COPY (
                        {testing_sql}
                    ) TO 'gs://{self.parquet_path}{self.table_name}_{str(uuid.uuid4())}{constants.PARQUET}' {constants.DUCKDB_FORMAT_STRING}
                """
                resave_no_return  =resave_statement.replace('\n','')
                utils.logger.warning(f"*/*/*/*/*/*/ resave is {resave_no_return}")
                conn.execute(resave_statement)
        except Exception as e:
            raise Exception(f"Unable to execute SQL to generate {self.table_name}: {str(e)}") from e
        finally:
            utils.close_duckdb_connection(conn, local_db_file)

    def perform_harmonization(self, step: str) -> None:
        """
        Perform a specific harmonization step.
        
        Args:
            step: The harmonization step to perform (e.g., SOURCE_TARGET)
        """
        if step == constants.SOURCE_TARGET:
            sql = self.get_source_target_mapping_sql()
            # The SQL will be used in the harmonize_parquet_file method
    
    def get_source_target_mapping_sql(self) -> str:
        """
        Generate SQL for mapping source targets based on vocabulary relationships.
        
        Returns:
            str: The generated SQL or an empty string if not applicable
        """
        utils.logger.warning(f"IN get_source_target_mapping_sql()")

        schema = utils.get_table_schema(self.table_name, self.cdm_version)

        columns = schema[self.table_name]["columns"]
        ordered_omop_columns = list(columns.keys())  # preserve column order

        # Get _concept_id and _source_concept_id columns for table
        target_concept_id_column = constants.SOURCE_TARGET_COLUMNS[self.table_name]['target_concept_id']
        source_concept_id_column = constants.SOURCE_TARGET_COLUMNS[self.table_name]['source_concept_id']
        primary_key = utils.get_primary_key_field(self.table_name, self.cdm_version)
        utils.logger.warning(f"target_concept_id_column is {target_concept_id_column} and source_concept_id_column is {source_concept_id_column} and primary_key is {primary_key}")

        # specimen and note tables don't have _source_concept_id columns so can't be evaluated with this method
        if not source_concept_id_column or source_concept_id_column == "":
            return ""

        initial_select_exprs: list = []
        final_select_exprs: list = []

        for column_name in ordered_omop_columns:
            column_name = f"tbl.{column_name}"
            final_select_exprs.append(column_name)

            # Replace new target concept_id in target_concept_id_column
            if column_name == target_concept_id_column:
                utils.logger.warning(f"REPLACED {column_name} ...")
                column_name = f"vocab.target_concept_id AS {target_concept_id_column}"
                utils.logger.warning(f"... with {column_name}")

            initial_select_exprs.append(column_name)
        
        # Add columns to store metadata related to vocab harmonization for later reporting
        metadata_columns = [
            "vocab.target_concept_id_domain AS target_domain",
            "'source concept available, target mapping available and not current; UPDATED' AS vocab_harmonization_status",
            f"tbl.{source_concept_id_column} AS source_concept_id",
            f"tbl.{target_concept_id_column} AS previous_target_concept_id",
            "vocab.target_concept_id AS updated_target_concept_id"
        ]
        for metadata_column in metadata_columns:
            initial_select_exprs.append(metadata_column)

            # Only include the alias in the second select statement
            alias = metadata_column.split(" AS ")[1]
            final_select_exprs.append(alias)

        initial_select_sql = ",\n                ".join(initial_select_exprs)

        initial_from_sql = f"""
            FROM read_parquet('@{self.table_name.upper()}') AS tbl
            INNER JOIN read_parquet('@OPTIMIZED_VOCABULARY') AS vocab
                ON tbl.{source_concept_id_column} = vocab.concept_id
            WHERE tbl.{source_concept_id_column} != 0
            AND tbl.{target_concept_id_column} != vocab.target_concept_id
            AND vocab.relationship_id IN ('Maps to', 'Maps to value', 'Maps to unit')
            AND vocab.target_concept_id_standard = 'S'
        """

        pivot_cte = f"""
            -- Pivot so that Meas Value mappings get associated with target_concept_id_field
            SELECT 
                tbl.{primary_key},
                MAX(vocab.target_concept_id) AS value_as_concept_id
            FROM read_parquet('@{self.table_name.upper()}') AS tbl
            INNER JOIN read_parquet('@OPTIMIZED_VOCABULARY') AS vocab 
                ON tbl.{source_concept_id_column} = vocab.concept_id
            WHERE vocab.target_concept_id_domain = 'Meas Value'
            GROUP BY tbl.{primary_key}
        """

        # Add column to final select that store Meas Value mapping
        final_select_exprs.append("mv_cte.value_as_concept_id")

        # Add target table to final output
        final_select_exprs.append(self.case_when_target_table)
        final_select_sql = ",\n                ".join(final_select_exprs)

        final_from_sql = f"""
            FROM base AS tbl
            LEFT JOIN meas_value AS mv_cte
                ON tbl.{primary_key} = mv_cte.{primary_key}
            WHERE tbl.target_domain != 'Meas Value'
        """

        final_cte = f"""
            WITH base AS (
                SELECT
                    {initial_select_sql}
                {initial_from_sql}
            ), meas_value AS (
                {pivot_cte}
            )
            SELECT
                {final_select_sql}
            {final_from_sql}
        """

        final_sql = utils.placeholder_to_table_path(
            self.site, 
            self.bucket, 
            self.delivery_date, 
            final_cte, 
            self.vocab_version, 
            self.vocab_gcs_bucket
        )

        final_sql_no_return = final_sql.replace('\n',' ')
        utils.logger.warning(f"*/*/*/ final_cte is {final_sql_no_return}")
        return final_sql