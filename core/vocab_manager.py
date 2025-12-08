import core.constants as constants
import core.gcp_services as gcp_services
import core.utils as utils
from core.storage_backend import storage


class VocabularyManager:
    """
    Manages OMOP vocabulary operations including conversion, optimization, and loading.

    Handles:
    - CSV to Parquet conversion for Athena vocabulary files
    - Creation of optimized vocabulary lookup file (denormalized)
    - Loading vocabulary tables to BigQuery
    """

    def __init__(self, vocab_version: str, vocab_path: str):
        """
        Initialize vocabulary manager.

        Args:
            vocab_version: Version of the vocabulary (e.g., 'v5.0_23-JAN-23')
            vocab_path: Root path where vocabulary files are stored
        """
        self.vocab_version = vocab_version
        self.vocab_path = vocab_path
        self.vocab_root_path = f"{vocab_path}/{vocab_version}/"
        self.optimized_vocab_folder_path = f"{self.vocab_root_path}{constants.OPTIMIZED_VOCAB_FOLDER}/"

    def convert_to_parquet(self) -> None:
        """
        Convert CSV vocabulary files from Athena to Parquet format.

        Processes all CSV vocabulary files in the vocabulary version directory,
        converting them to Parquet and storing in the optimized_vocab folder.
        Handles date fields (valid_start_date, valid_end_date) with proper formatting.

        Raises:
            Exception: If vocabulary path not found or conversion fails
        """
        # Confirm desired vocabulary version exists
        vocab_files = utils.list_files(self.vocab_path, self.vocab_version, constants.CSV)

        if not vocab_files:
            raise Exception(f"Vocabulary path {self.vocab_root_path} not found")

        for vocab_file in vocab_files:
            vocab_file_name = vocab_file.replace(constants.CSV, '').lower()
            parquet_file_path = f"{self.optimized_vocab_folder_path}{vocab_file_name}{constants.PARQUET}"
            csv_file_path = f"{self.vocab_root_path}{vocab_file}"

            # Continue only if the vocabulary file has not been created or is not valid
            if not utils.parquet_file_exists(parquet_file_path) or not utils.valid_parquet_file(parquet_file_path):
                # Get column names
                csv_columns = utils.get_columns_from_file(csv_file_path)

                # Generate SQL
                convert_query = self.generate_convert_vocab_sql(csv_file_path, parquet_file_path, csv_columns)

                # Execute SQL
                utils.execute_duckdb_sql(convert_query, "Unable to convert vocabulary CSV to Parquet")

    def create_optimized_vocab_file(self) -> None:
        """
        Create optimized vocabulary file by denormalizing concept and concept_relationship tables.

        Combines concept IDs with their mapping/replacement relationships into a single
        table/file for efficient lookups during vocabulary harmonization.

        Raises:
            Exception: If vocabulary path not found or file creation fails
        """
        optimized_file_path = utils.get_optimized_vocab_file_path(self.vocab_version, self.vocab_path)

        # Create the optimized vocabulary file if it doesn't exist
        if utils.parquet_file_exists(optimized_file_path):
            return

        # Ensure existing vocab file can be read
        if not utils.valid_parquet_file(optimized_file_path):
            # Ensure vocabulary version actually exists by checking if concept file exists
            concept_check_path = f"{self.optimized_vocab_folder_path}concept{constants.PARQUET}"

            if not storage.file_exists(concept_check_path):
                raise Exception(f"Vocabulary path {self.vocab_root_path} not found")

            # Build paths for read_parquet statements
            concept_path = storage.get_uri(f"{self.optimized_vocab_folder_path}concept{constants.PARQUET}")
            concept_relationship_path = storage.get_uri(f"{self.optimized_vocab_folder_path}concept_relationship{constants.PARQUET}")
            output_path = storage.get_uri(optimized_file_path)

            # Generate SQL
            transform_query = self.generate_optimized_vocab_sql(concept_path, concept_relationship_path, output_path)

            # Execute SQL
            utils.execute_duckdb_sql(transform_query, "Unable to create optimized vocab file")

    def load_vocabulary_table_to_bq(self, table_file_name: str, project_id: str, dataset_id: str) -> None:
        """
        Load vocabulary Parquet file to BigQuery table.

        Args:
            table_file_name: Name of the vocabulary table file (without extension)
            project_id: GCP project ID for BigQuery
            dataset_id: BigQuery dataset ID

        Raises:
            Exception: If vocabulary table not found or load fails
        """
        vocab_parquet_path = storage.get_uri(
            f"{self.optimized_vocab_folder_path}{table_file_name}{constants.PARQUET}"
        )

        if not utils.parquet_file_exists(vocab_parquet_path) or not utils.valid_parquet_file(vocab_parquet_path):
            raise Exception(f"Vocabulary table {table_file_name} not found at {vocab_parquet_path}")

        gcp_services.load_parquet_to_bigquery(
            vocab_parquet_path,
            project_id,
            dataset_id,
            table_file_name,
            constants.BQWriteTypes.SPECIFIC_FILE
        )

    @staticmethod
    def generate_vocab_version_query_sql(vocabulary_file_path: str) -> str:
        """
        Generate SQL to extract vocabulary version from vocabulary table.

        Args:
            vocabulary_file_path: Full URI path to the vocabulary parquet file

        Returns:
            SQL statement that queries vocabulary_version for the 'None' vocabulary_id
        """
        return f"""
        SELECT vocabulary_version
        FROM read_parquet('{vocabulary_file_path}')
        WHERE vocabulary_id = 'None'
    """

    @staticmethod
    def generate_convert_vocab_sql(csv_file_path: str, parquet_file_path: str, csv_columns: list[str]) -> str:
        """
        Generate SQL to convert vocabulary CSV file to Parquet format.

        Creates SQL that:
        - Reads tab-delimited CSV vocabulary file
        - Handles date fields (valid_start_date, valid_end_date) with proper formatting
        - Preserves column names and order from CSV

        Args:
            csv_file_path: Path to the input CSV vocabulary file
            parquet_file_path: Path for the output Parquet file
            csv_columns: List of column names from the CSV file

        Returns:
            SQL string for converting vocabulary CSV to Parquet
        """
        # Build the SELECT statement with columns in the predefined order
        select_columns = []
        for col in csv_columns:
            if col in ('valid_start_date', 'valid_end_date'):
                # Handle date fields; need special handling or they're interpreted as numeric values
                select_columns.append(
                    f'CAST(STRPTIME(CAST("{col}" AS VARCHAR), \'%Y%m%d\') AS DATE) AS "{col}"'
                )
            else:
                select_columns.append(f'"{col}"')

        select_statement = ', '.join(select_columns)

        # Execute the COPY command to convert CSV to Parquet with columns in the correct order
        return f"""
        COPY (
            SELECT {select_statement}
            FROM read_csv('{storage.get_uri(csv_file_path)}', delim='\t',strict_mode=False)
        ) TO '{storage.get_uri(parquet_file_path)}' {constants.DUCKDB_FORMAT_STRING};
    """

    @staticmethod
    def generate_optimized_vocab_sql(concept_path: str, concept_relationship_path: str, output_path: str) -> str:
        """
        Generate SQL to create optimized vocabulary file.

        Creates SQL that:
        - Denormalizes concept and concept_relationship tables
        - Includes mapping relationships (Maps to, Maps to value, Maps to unit)
        - Includes replacement relationships (Concept replaced by, Was a to, etc.)
        - Outputs to optimized vocabulary file for efficient lookups

        Args:
            concept_path: URI path to concept.parquet file
            concept_relationship_path: URI path to concept_relationship.parquet file
            output_path: URI path for output optimized_vocab_file.parquet

        Returns:
            SQL string for creating optimized vocabulary file
        """
        return f"""
                COPY (
                    SELECT DISTINCT
                        c1.concept_id AS concept_id, -- Every concept_id from concept table
                        c1.standard_concept AS concept_id_standard,
                        c1.domain_id AS concept_id_domain,
                        cr.relationship_id,
                        cr.concept_id_2 AS target_concept_id, -- targets to concept_id's
                        c2.standard_concept AS target_concept_id_standard,
                        c2.domain_id AS target_concept_id_domain
                    FROM read_parquet('{concept_path}') c1
                    LEFT JOIN read_parquet('{concept_relationship_path}') cr on c1.concept_id = cr.concept_id_1
                    LEFT JOIN read_parquet('{concept_path}') c2 on cr.concept_id_2 = c2.concept_id
                    WHERE IFNULL(cr.relationship_id, '')
                        IN ('', {constants.MAPPING_RELATIONSHIPS},{constants.REPLACEMENT_RELATIONSHIPS})
                ) TO '{output_path}' {constants.DUCKDB_FORMAT_STRING}
                """
