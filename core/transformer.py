import re

import core.constants as constants
import core.utils as utils
from core.storage_backend import storage


class Transformer:
    """
    Class for performing OMOP to OMOP ETLs. Required as part of vocabulary harmonization
    The domain_id of a concept may change between different vocabulary versions, and data 
    must be moved to the table appropriate for their new domain.
    """
    def __init__(self, site: str, file_path: str, cdm_version: str, source_table: str, target_table: str, etl_artifact_path: str):
        """Initialize Transformer object used for OMOP-to-OMOP ETL."""
        self.site = site
        self.file_path = file_path # Path to vocabulary-harmonized Parquet file
        self.cdm_version = cdm_version
        self.source_table = source_table
        self.target_table = target_table
        self.etl_artifact_path = etl_artifact_path

    def omop_to_omop_etl(self) -> None:
        """Execute OMOP-to-OMOP ETL transformation using SQL scripts."""
        transform_sql = self.generate_omop_to_omop_sql()
        utils.execute_duckdb_sql(transform_sql, f"Unable to execute OMOP ETL SQL transformation")

    def generate_omop_to_omop_sql(self) -> str:
        """
        Generate a SQL statement that transforms data from one OMOP table to another,
        ensuring proper column types and adding placeholder values to NULL required columns

        NOTE: Unlike other SQL generation functions in the codebase, this is NOT a static method.
        It requires many instance state (cdm_version, source_table, target_table, site,
        file_path, etl_artifact_path) attributes throughout its logic. The Transformer class is 
        designed as a stateful context object for transformation operations.
        """
        
        # Find the transform SQL file
        transform_file = f"{constants.OMOP_ETL_SCRIPT_PATH}{self.cdm_version}/{self.source_table}_to_{self.target_table}.sql"
        
        # Read the transform SQL
        with open(transform_file, 'r') as f:
            sql = f.read()
        
        # Load the target table schema
        schema = utils.get_table_schema(self.target_table, self.cdm_version)
        
        # Keep the full columns dictionary
        target_columns_dict = schema[self.target_table]["columns"]
        
        # Identify primary key column of the target table
        primary_key_column = utils.get_primary_key_column(self.target_table, self.cdm_version)
        
        # Parse the SQL and collect all column mappings
        lines = sql.split('\n')
        column_mappings = []  # Will store (source_expr, target_column) tuples
        
        in_select = False
        for line in lines:
            line_stripped = line.strip()
            
            # Handle SELECT line
            if line_stripped.upper().startswith('SELECT'):
                in_select = True
                continue
            
            # Handle FROM line
            if line_stripped.upper().startswith('FROM'):
                in_select = False
                continue
            
            # Skip non-mapping lines
            if not in_select or not line_stripped:
                continue
            
            # Handle column mapping
            has_comma = line_stripped.endswith(',')
            if has_comma:
                line_stripped = line_stripped[:-1]
            
            # Split into source expression and target column
            parts = re.split(r'\s+AS\s+', line_stripped, flags=re.IGNORECASE)
            if len(parts) != 2:
                continue
            
            source_expr, target_column = parts
            source_expr = source_expr.strip()
            target_column = target_column.strip()
            
            column_mappings.append((source_expr, target_column))
        
        # Now process the SQL again, applying transformations and the primary key replacement
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
            
            # Apply primary key replacement if this is the primary key column
            if primary_key_column and target_column == primary_key_column and self.target_table in constants.SURROGATE_KEY_TABLES:
                # Generate a composite key using all source expressions
                concat_parts = []
                for src_expr, _ in column_mappings:
                    concat_parts.append(f"CAST({src_expr} AS VARCHAR)")
                
                # Add site name to make primary key unique to values and site
                concat_parts.append(f"'{self.site}'")

                # Create the concatenation expression
                concat_expr = "CONCAT(" + ",".join(concat_parts) + ")"
                # hash returns unsigned value that may exceed signed INT64 space
                # Use % 9223372036854775807 to ensure value fits in signed INT64 space
                source_expr = f"hash({concat_expr}) % 9223372036854775807"
            
            # Get target column schema info - check if the column exists in the schema
            if target_column in target_columns_dict:
                column_info = target_columns_dict[target_column]

                column_type = column_info['type']
                is_required = column_info['required'].lower() == 'true'

                # Concept_id fields must ALWAYS default to 0, never NULL (OHDSI convention)
                # Apply COALESCE to all concept_id fields regardless of required status
                if target_column.endswith('_concept_id'):
                    placeholder = utils.get_placeholder_value(target_column, column_type)
                    if is_required:
                        final_expr = f"CAST(COALESCE({source_expr}, {placeholder}) AS {column_type})"
                    else:
                        final_expr = f"TRY_CAST(COALESCE({source_expr}, {placeholder}) AS {column_type})"
                # Process other fields based on whether they're required or not
                elif is_required:
                    # For required non-concept_id fields: COALESCE first, then CAST
                    placeholder = utils.get_placeholder_value(target_column, column_type)
                    final_expr = f"CAST(COALESCE({source_expr}, {placeholder}) AS {column_type})"
                else:
                    # For non-required non-concept_id fields: TRY_CAST only
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

        transform_sql = f"""
            COPY (
                {final_sql}
                WHERE target_table = '{self.target_table}'
            ) TO '{storage.get_uri(self.get_transformed_path())}' {constants.DUCKDB_FORMAT_STRING}
        """

        return transform_sql

    def get_transformed_path(self) -> str:
        """Return output path for transformed Parquet file."""
        return f"{self.etl_artifact_path}{self.target_table}/parts/{self.target_table}_from_{self.source_table}{constants.PARQUET}"

    def placeholder_to_file_path(self, sql: str) -> str:
        """
        Replaces clinical data table place holder strings in SQL scripts with paths to table parquet files
        """
        replacement_result = sql

        for placeholder, _ in constants.CLINICAL_DATA_PATH_PLACEHOLDERS.items():
            clinical_data_table_path = storage.get_uri(f"{self.file_path}*{constants.PARQUET}")
            replacement_result = replacement_result.replace(placeholder, clinical_data_table_path)

        return replacement_result