import core.constants as constants
import core.utils as utils
import core.file_processor as fp
import uuid
from typing import Optional
import re

class VocabHarmonizer:
    """
    A class for harmonizing parquet files according to specified vocabularies and mappings.
    Handles the entire process from reading parquet files to generating SQL and saving the harmonized output.
    """
    
    def __init__(self, gcs_file_path: str, cdm_version: str, site: str, vocab_version: str, vocab_gcs_bucket: str, target_table: Optional[str] = 'measurement'):
        """
        Initialize a VocabHarmonizer with common parameters needed across all operations.
        """
        self.gcs_file_path = gcs_file_path
        self.cdm_version = cdm_version
        self.site = site
        self.vocab_version = vocab_version
        self.vocab_gcs_bucket = vocab_gcs_bucket
        self.source_table_name = utils.get_table_name_from_gcs_path(gcs_file_path)
        self.target_table_name = target_table
        self.bucket = utils.get_bucket_and_delivery_date_from_gcs_path(gcs_file_path)[0]
        self.delivery_date = utils.get_bucket_and_delivery_date_from_gcs_path(gcs_file_path)[1]
        self.source_parquet_path = utils.get_parquet_artifact_location(gcs_file_path)
        self.target_parquet_path = utils.get_parquet_harmonized_path(gcs_file_path)
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
            ELSE '{self.source_table_name}' END AS target_table
        """
    
    def harmonize_parquet_file(self) -> None:
        """
        Harmonize a parquet file by applying defined harmonization steps and saving the result.
        """

        # List order is very important here!
        harmonization_steps: list = [constants.SOURCE_TARGET]

        for step in harmonization_steps:
            self.perform_harmonization(step)

        # After finding new targets and domain, partition files based on target OMOP table
        self.partition_by_target_table()

        # Transform source table structure to target table structure
        # TODO: Call this on each partitioned part indepdenently
        self.omop_to_omop_etl()


    def omop_to_omop_etl(self) -> None:
        """
        Generate a SQL statement that transforms data from one OMOP table to another,
        ensuring proper column types and adding placeholder values to NULL required columns
        """
        
        # Find the transform SQL file
        transform_file = f"{constants.OMOP_ETL_PATH}{self.source_table_name}_to_{self.target_table_name}.sql"
        
        # Read the transform SQL
        with open(transform_file, 'r') as f:
            sql = f.read()
        
        # Load the target table schema
        schema = utils.get_table_schema(self.target_table_name, constants.CDM_v54)
        
        # Keep the full columns dictionary
        target_columns_dict = schema[self.target_table_name]["columns"]
        
        # Parse the SQL and modify each column
        lines = sql.split('\n')
        modified_lines = []
        
        in_select = False
        for line in lines:
            line_stripped = line.strip()
            
            # Handle SELECT line
            if line_stripped.upper().startswith('SELECT'):
                in_select = True
                modified_lines.append(line)
                continue
            
            # Handle FROM line
            if line_stripped.upper().startswith('FROM'):
                in_select = False
                modified_lines.append(line)
                continue
            
            # Skip non-mapping lines
            if not in_select or not line_stripped:
                modified_lines.append(line)
                continue
            
            # Handle column mapping
            has_comma = line_stripped.endswith(',')
            if has_comma:
                line_stripped = line_stripped[:-1]
            
            # Split into source expression and target column
            parts = re.split(r'\s+AS\s+', line_stripped, flags=re.IGNORECASE)
            if len(parts) != 2:
                modified_lines.append(line)
                continue
            
            source_expr, target_column = parts
            source_expr = source_expr.strip()
            target_column = target_column.strip()
            
            # Get target column schema info - check if the column exists in the schema
            if target_column in target_columns_dict:
                column_info = target_columns_dict[target_column]
                
                column_type = column_info['type']
                is_required = column_info['required'].lower() == 'true'
                
                # Process differently based on whether field is required or not
                if is_required:
                    # For required fields: COALESCE first, then CAST
                    placeholder = fp.get_placeholder_value(target_column, column_type)
                    final_expr = f"CAST(COALESCE({source_expr}, {placeholder}) AS {column_type})"
                else:
                    # For non-required fields: TRY_CAST only
                    final_expr = f"TRY_CAST({source_expr} AS {column_type})"
                
                # Recreate the line with proper indentation
                indent = len(line) - len(line.lstrip())
                modified_line = ' ' * indent + final_expr + ' AS ' + target_column
                if has_comma:
                    modified_line += ','
                
                modified_lines.append(modified_line)
            else:
                # If column not in schema, keep as is
                modified_lines.append(line)
        
        # Join the lines together to form the final SQL
        select_sql = '\n'.join(modified_lines)

        # Replace placeholder table strings with paths to Parquet files
        final_sql = self.placeholder_to_file_path(select_sql)
        
        # Log the SQL without newlines for easier viewing
        final_sql_no_return = final_sql.replace('\n', ' ')
        utils.logger.warning(f"TRANSFORM SQL IS {final_sql_no_return}")


    def placeholder_to_file_path(self, sql: str) -> str:
        """
        Replaces clinical data table place holder strings in SQL scripts with paths to table parquet files
        """
        replacement_result = sql

        for placeholder, _ in constants.CLINICAL_DATA_PATH_PLACEHOLDERS.items():
            clinical_data_table_path = f"read_parquet('gs://{self.target_parquet_path}partitioned/target_table={self.target_table_name}/*.parquet')"
            replacement_result = replacement_result.replace(placeholder, clinical_data_table_path)

        return replacement_result

    def perform_harmonization(self, step: str) -> None:
        """
        Perform a specific harmonization step.
        """
        if step == constants.SOURCE_TARGET:
            self.source_target_remapping()
 
    # Keys which have already been reprocessed
    def get_already_processed_primary_keys() -> str:
        print()

    def execute_duckdq_sql(self, sql: str, error_msg: str) -> None:
        try:
            conn, local_db_file = utils.create_duckdb_connection()

            with conn:
                sql_no_return = sql.replace('\n',' ')
                utils.logger.warning(f"SQL is {sql_no_return}")
                conn.execute(sql)
                utils.logger.warning(f"DID EXECUTE THE SQL!")
        except Exception as e:
            raise Exception(f"{error_msg}: {str(e)}") from e
        finally:
            utils.close_duckdb_connection(conn, local_db_file)        

    def source_target_remapping(self) -> None:
        """
        Generate and execute SQL to check for and update non-standard source-to-target mappings to standard        
        """

        schema = utils.get_table_schema(self.source_table_name, self.cdm_version)

        columns = schema[self.source_table_name]["columns"]
        ordered_omop_columns = list(columns.keys())  # preserve column order

        # Get _concept_id and _source_concept_id columns for table
        target_concept_id_column = constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['target_concept_id']
        source_concept_id_column = constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['source_concept_id']
        primary_key = utils.get_primary_key_column(self.source_table_name, self.cdm_version)

        # specimen and note tables don't have _source_concept_id columns so can't be evaluated with this method
        if not source_concept_id_column or source_concept_id_column == "":
            return

        initial_select_exprs: list = []
        final_select_exprs: list = []

        for column_name in ordered_omop_columns:
            column_name = f"tbl.{column_name}"
            final_select_exprs.append(column_name)

            # Replace new target concept_id in target_concept_id_column
            if column_name == f"tbl.{target_concept_id_column}":
                column_name = f"vocab.target_concept_id AS {target_concept_id_column}"

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
            FROM read_parquet('@{self.source_table_name.upper()}') AS tbl
            INNER JOIN read_parquet('@OPTIMIZED_VOCABULARY') AS vocab
                ON tbl.{source_concept_id_column} = vocab.concept_id
            WHERE tbl.{source_concept_id_column} != 0
            AND tbl.{target_concept_id_column} != vocab.target_concept_id
            AND vocab.relationship_id IN ('Maps to', 'Maps to value', 'Maps to unit')
            AND vocab.target_concept_id_standard = 'S'
        """

        pivot_cte = f"""
            -- Pivot so that Meas Value mappings get associated with target_concept_id_column
            SELECT 
                tbl.{primary_key},
                MAX(vocab.target_concept_id) AS value_as_concept_id
            FROM read_parquet('@{self.source_table_name.upper()}') AS tbl
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

        cte_with_placeholders = f"""
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

        final_cte = utils.placeholder_to_file_path(
            self.site, 
            self.bucket, 
            self.delivery_date, 
            cte_with_placeholders, 
            self.vocab_version, 
            self.vocab_gcs_bucket
        )

        final_sql = f"""
            COPY (
                {final_cte}
            ) TO 'gs://{self.target_parquet_path}{self.source_table_name}_source_target_remap{constants.PARQUET}' {constants.DUCKDB_FORMAT_STRING}
        """

        self.execute_duckdq_sql(final_sql, f"Unable to execute SQL to harominze vocabulary in table {self.source_table_name}")
    
    def partition_by_target_table(self) -> None:
        partition_statement = f"""
            COPY (
                SELECT * FROM read_parquet('gs://{self.target_parquet_path}*{constants.PARQUET}')
            ) TO 'gs://{self.target_parquet_path}partitioned/' (FORMAT PARQUET, PARTITION_BY (target_table), COMPRESSION ZSTD);
        """
        
        self.execute_duckdq_sql(partition_statement, f"Unable to partition file {self.source_table_name}")

    