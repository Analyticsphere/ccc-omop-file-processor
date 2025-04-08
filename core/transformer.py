import logging
import re
import sys

import core.constants as constants
import core.file_processor as fp
import core.utils as utils


class Transformer:
    """
    Class for performing OMOP to OMOP ETLs. Required as part of vocabulary harmonization
    The domain_id of a concept may change between different vocabulary versions, and data 
    must be moved to the table appropriate for their new domain.
    """
    def __init__(self, site: str, file_path: str, cdm_version: str, source_table: str, target_table: str):
        self.site = site
        self.file_path = file_path
        self.cdm_version = cdm_version
        self.source_table = source_table
        self.target_table = target_table
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        # Create the logger at module level so its settings are applied throughout class
        self.logger = logging.getLogger(__name__)

    def generate_omop_to_omop_sql(self) -> str:
        """
        Generate a SQL statement that transforms data from one OMOP table to another,
        ensuring proper column types and adding placeholder values to NULL required columns
        """
        
        # Find the transform SQL file
        transform_file = f"{constants.OMOP_ETL_PATH}{self.cdm_version}/{self.source_table}_to_{self.target_table}.sql"
        
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

        transform_sql = f"""
            COPY (
                {final_sql}
                WHERE target_table = '{self.target_table}'
            ) TO 'gs://{self.get_transformed_path()}' {constants.DUCKDB_FORMAT_STRING}
        """

        return transform_sql


    def get_transformed_path(self) -> str:
        return f"{self.file_path}transformed/{self.target_table}{constants.PARQUET}"


    def omop_to_omop_etl(self) -> None:
        # Execute the OMOP to OMOP ETL SQL script
        transform_sql = self.generate_omop_to_omop_sql()
        utils.execute_duckdb_sql(transform_sql, f"Unable to execute OMOP ETL SQL transformation")

        # Resolve duplicate primary keys within a single 'table part' file
        # TODO: Make this global across ALL table parts
        #self.handle_duplicate_primary_keys()


    def placeholder_to_file_path(self, sql: str) -> str:
        """
        Replaces clinical data table place holder strings in SQL scripts with paths to table parquet files
        """
        replacement_result = sql

        for placeholder, _ in constants.CLINICAL_DATA_PATH_PLACEHOLDERS.items():
            clinical_data_table_path = f"gs://{self.file_path}*{constants.PARQUET}"
            replacement_result = replacement_result.replace(placeholder, clinical_data_table_path)

        return replacement_result


    def handle_duplicate_primary_keys(self) -> None:
        """
        Among tables with surrogate keys, checks for duplicate primary keys in the transformed parquet file 
        and replaces them with new deterministic keys.
        
        For duplicate primary keys, all instances except the first are replaced with new keys.
        The new keys are generated by hashing the original key + row_id (from a window function).
        
        Example:
        If we have three instances of the key 12345:
        original keys: 12345, 12345, 12345
        after processing: 12345, hash(123452), hash(123453)
        
        This ensures that the new keys are deterministic (same input always gives same output)
        and unique across the table.
        """
        if self.target_table in constants.SURROGATE_KEY_TABLES:
            self.logger.info(f"Checking for and correcting duplicate primary keys in {self.get_transformed_path()}")
            # Get the path to the transformed parquet file
            transformed_file_path = f"gs://{self.get_transformed_path()}"
            
            # Identify primary key column of the target table
            primary_key_column = utils.get_primary_key_column(self.target_table, self.cdm_version)
            
            if not primary_key_column:
                self.logger.warning(f"No primary key column defined for table {self.target_table}. Skipping duplicate check.")
                return
            
            # Load the target table schema to get the type of the primary key column
            schema = utils.get_table_schema(self.target_table, self.cdm_version)
            if self.target_table not in schema:
                self.logger.warning(f"Schema not found for table {self.target_table}. Skipping duplicate check.")
                return
                
            target_columns_dict = schema[self.target_table]["columns"]
            
            if primary_key_column not in target_columns_dict:
                self.logger.warning(f"Primary key column {primary_key_column} not found in schema for table {self.target_table}. Skipping duplicate check.")
                return
                
            primary_key_type = target_columns_dict[primary_key_column]['type']
            
            # SQL to fix duplicates
            fix_sql = f"""
            -- Create a temporary table to store duplicate detection results
            CREATE TEMPORARY TABLE duplicate_keys AS
            SELECT {primary_key_column}
            FROM '{transformed_file_path}'
            GROUP BY {primary_key_column}
            HAVING COUNT(*) > 1;
            
            -- Create a temporary table to store the data with row numbers
            CREATE TEMPORARY TABLE data_with_row_nums AS
            SELECT t.*, 
                CASE 
                    WHEN d.{primary_key_column} IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY t.{primary_key_column})
                    ELSE 1
                END AS row_id
            FROM '{transformed_file_path}' t
            LEFT JOIN duplicate_keys d ON t.{primary_key_column} = d.{primary_key_column};
            
            -- Create a temporary table with the new primary keys
            CREATE TEMPORARY TABLE data_with_new_keys AS
            SELECT 
                CASE 
                    WHEN row_id = 1 THEN {primary_key_column}  -- Keep first instance unchanged
                    ELSE CAST(hash(CONCAT(CAST({primary_key_column} AS VARCHAR), CAST(row_id AS VARCHAR))) % 9223372036854775807 AS {primary_key_type})  -- Generate new key for duplicates
                END AS new_primary_key,
                *
            FROM data_with_row_nums;
            
            -- Write the fixed data back to the original file (replacing old primary key with new one)
            COPY (
                SELECT 
                    new_primary_key AS {primary_key_column},
                    * EXCLUDE (new_primary_key, row_id, {primary_key_column})
                FROM data_with_new_keys
            ) TO '{transformed_file_path}' {constants.DUCKDB_FORMAT_STRING};
            """
            
            # Execute the SQL to handle duplicates
            utils.execute_duckdb_sql(fix_sql, f"Unable to handle duplicate primary keys in {self.target_table}")

