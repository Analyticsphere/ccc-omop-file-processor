import core.constants as constants
import core.utils as utils
from typing import Optional
import re

class VocabHarmonizer:
    """
    A class for harmonizing parquet files according to specified vocabularies and mappings.
    Handles the entire process from reading parquet files to generating SQL and saving the harmonized output.
    """
    
    def __init__(self, gcs_file_path: str, cdm_version: str, site: str, vocab_version: str, vocab_gcs_bucket: str):
        """
        Initialize a VocabHarmonizer with common parameters needed across all operations.
        """
        self.logger = utils.logger
        self.gcs_file_path = gcs_file_path
        self.cdm_version = cdm_version
        self.site = site
        self.vocab_version = vocab_version
        self.vocab_gcs_bucket = vocab_gcs_bucket
        self.source_table_name = utils.get_table_name_from_gcs_path(gcs_file_path)
        self.bucket = utils.get_bucket_and_delivery_date_from_gcs_path(gcs_file_path)[0]
        self.delivery_date = utils.get_bucket_and_delivery_date_from_gcs_path(gcs_file_path)[1]
        self.source_parquet_path = utils.get_parquet_artifact_location(gcs_file_path)
        self.target_parquet_path = utils.get_parquet_harmonized_path(gcs_file_path)
        


    def update_mappings_for_file(self) -> None:
        """
        Harmonize a parquet file by applying defined harmonization steps and saving the result.
        """

        # List order is very important here!
        harmonization_steps: list = [constants.SOURCE_TARGET, constants.DOMAIN_CHECK]

        for step in harmonization_steps:
            self.perform_harmonization(step)


        # After finding new targets and domain, partition file based on target OMOP table
        self.partition_by_target_table()


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
            "vocab.target_concept_id AS target_concept_id"
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
        case_when_target_table = f"""
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
        final_select_exprs.append(case_when_target_table)
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

        utils.execute_duckdq_sql(final_sql, f"Unable to execute SQL to harominze vocabulary in table {self.source_table_name}")


    def domain_table_check(self) -> None:

        self.logger.warning(f"!!! GOING TO PERFORM DOMAIN TABLE CHECK")
        schema = utils.get_table_schema(self.source_table_name, self.cdm_version)

        columns = schema[self.source_table_name]["columns"]
        ordered_omop_columns = list(columns.keys())  # preserve column order
        target_concept_id_column = f"tbl.{constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['target_concept_id']}"
        source_concept_id_column = '0' if constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['source_concept_id'] == "" \
            else f"tbl.{constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['source_concept_id']}"
        primary_key_column = utils.get_primary_key_column(self.source_table_name, self.cdm_version)

        select_exprs: list = []

        for column_name in ordered_omop_columns:
            column_name = f"tbl.{column_name}"
            select_exprs.append(column_name)

        # Add columns to store metadata related to vocab harmonization for later reporting
        metadata_columns = [
            "vocab.concept_id_domain AS target_domain",
            "'domain check' AS vocab_harmonization_status",
            f"{source_concept_id_column} AS source_concept_id",
            f"{target_concept_id_column} AS previous_target_concept_id",
            f"{target_concept_id_column} AS target_concept_id"
        ]
        for metadata_column in metadata_columns:
            select_exprs.append(metadata_column)
        
        # Add value_as_concept_id field to keep structure consistent with remapped tables
        select_exprs.append("CAST(NULL AS BIGINT) AS value_as_concept_id")
        # Add target table statement
        case_when_target_table = f"""
            CASE 
                WHEN vocab.concept_id_domain = 'Visit' THEN 'visit_occurrence'
                WHEN vocab.concept_id_domain = 'Condition' THEN 'condition_occurrence'
                WHEN vocab.concept_id_domain = 'Drug' THEN 'drug_exposure'
                WHEN vocab.concept_id_domain = 'Procedure' THEN 'procedure_occurrence'
                WHEN vocab.concept_id_domain = 'Device' THEN 'device_exposure'
                WHEN vocab.concept_id_domain = 'Measurement' THEN 'measurement'
                WHEN vocab.concept_id_domain = 'Observation' THEN 'observation'
                WHEN vocab.concept_id_domain = 'Note' THEN 'note'
                WHEN vocab.concept_id_domain = 'Specimen' THEN 'specimen'
            ELSE '{self.source_table_name}' END AS target_table
        """
        select_exprs.append(case_when_target_table)

        select_sql = ",\n                ".join(select_exprs)

        from_sql = f"""
            FROM read_parquet('@{self.source_table_name.upper()}') AS tbl
            INNER JOIN read_parquet('@OPTIMIZED_VOCABULARY') AS vocab
                ON {target_concept_id_column} = vocab.concept_id
        """

        # Don't perform domain check on rows which have already been harominzed
        # primary_key_column values were made unique per row values in normalization step, 
        #   so they can be used for identification here
        exisiting_files = utils.valid_parquet_file(f'{self.target_parquet_path}*{constants.PARQUET}')
        where_sql = ""
        if exisiting_files:
            where_sql = f"""
                WHERE tbl.{primary_key_column} NOT IN (
                    SELECT {primary_key_column} FROM read_parquet('gs://{self.target_parquet_path}*{constants.PARQUET}')
                )
            """

        sql_statement = f"""
            COPY (
                SELECT {select_sql}
                {from_sql}
                {where_sql}
            ) TO 'gs://{self.target_parquet_path}{self.source_table_name}_domain_check{constants.PARQUET}' {constants.DUCKDB_FORMAT_STRING}
        """

        final_sql_statement = utils.placeholder_to_file_path(
            self.site,
            self.bucket,
            self.delivery_date,
            sql_statement,
            self.vocab_version,
            self.vocab_gcs_bucket
        )

        utils.execute_duckdq_sql(final_sql_statement, f"Unable to perform domain check against {self.source_table_name}")







    def partition_by_target_table(self) -> None:
        
        self.logger.info(f"Partitioning table {self.source_table_name} for {self.site}")
        # There's a bug in DuckDB/fsspec that causes Hive partitioning to hang or OOM when working with remote file systems
        # As a workaround, manually partitioning the files in the same way DuckDB would
        # https://github.com/duckdb/duckdb/issues/11817
        # https://github.com/duckdb/duckdb/issues/8981
        # partition_statement = f"""
        #     COPY (
        #         SELECT * FROM read_parquet('gs://{self.target_parquet_path}*{constants.PARQUET}')
        #     ) TO 'gs://{self.target_parquet_path}partitioned/' (FORMAT PARQUET, PARTITION_BY (target_table), COMPRESSION ZSTD);
        # """

        # Find all target tables in the source file
        target_tables = f"""
            SELECT DISTINCT table_table FROM read_parquet('gs://{self.target_parquet_path}*{constants.PARQUET}')
        """
        conn, local_db_file = utils.create_duckdb_connection()

        try:
            with conn:
                target_tables_list = conn.execute(target_tables).fetch_df()['table_table'].tolist()
        except Exception as e:
            raise Exception(f"Unable to get target tables from Parquet file: {e}") from e
        finally:
            utils.close_duckdb_connection(conn, local_db_file)
        
        # Create a new Parquet file for each target table, using data_0 as file name (like DuckDB would)
        for target_table in target_tables_list:
            file_path = f"{self.target_parquet_path}partitioned/target_table={target_table}/data_0{constants.PARQUET}"
            partition_statement = f"""
                COPY (
                    SELECT * FROM read_parquet('gs://{self.target_parquet_path}*{constants.PARQUET}')
                    WHERE target_table = '{target_table}'
                ) TO 'gs://{file_path}' {constants.DUCKDB_FORMAT_STRING};
            """
            utils.execute_duckdq_sql(partition_statement, f"Unable to partition file {self.source_table_name}")


    def perform_harmonization(self, step: str) -> None:
        """
        Perform a specific harmonization step.
        """
        if step == constants.SOURCE_TARGET:
            self.source_target_remapping()
        elif step == constants.DOMAIN_CHECK:
            self.domain_table_check()
        else:
            return