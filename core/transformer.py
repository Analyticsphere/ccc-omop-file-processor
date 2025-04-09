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
        self.handle_duplicate_surrogate_primary_keys()


    def placeholder_to_file_path(self, sql: str) -> str:
        """
        Replaces clinical data table place holder strings in SQL scripts with paths to table parquet files
        """
        replacement_result = sql

        for placeholder, _ in constants.CLINICAL_DATA_PATH_PLACEHOLDERS.items():
            clinical_data_table_path = f"gs://{self.file_path}*{constants.PARQUET}"
            replacement_result = replacement_result.replace(placeholder, clinical_data_table_path)

        return replacement_result


    def handle_duplicate_surrogate_primary_keys(self) -> None:
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
        if self.target_table not in constants.SURROGATE_KEY_TABLES:
            self.logger.info(f"Table {self.target_table} does not use surrogate keys. Skipping duplicate check.")
            return
                
        transformed_file_path = f"gs://{self.get_transformed_path()}"
        
        # Identify primary key column
        primary_key_column = utils.get_primary_key_column(self.target_table, self.cdm_version)
        if not primary_key_column:
            self.logger.info(f"No primary key column defined for table {self.target_table}. Skipping duplicate check.")
            return
        
        # Get primary key data type
        schema = utils.get_table_schema(self.target_table, self.cdm_version)
        if self.target_table not in schema:
            self.logger.error(f"Schema not found for table {self.target_table}. Skipping duplicate check.")
            return
        
        target_columns_dict = schema[self.target_table]["columns"]
        if primary_key_column not in target_columns_dict:
            self.logger.error(f"Primary key column {primary_key_column} not found in schema. Skipping duplicate check.")
            return
            
        primary_key_type = target_columns_dict[primary_key_column]['type']
        
        try:
            conn, local_db_file = utils.create_duckdb_connection()
            
            with conn:
                # Step 1: Check if duplicates exist at all
                check_sql = f"""
                SELECT 
                    {primary_key_column}, 
                    COUNT(*) as dup_count
                FROM '{transformed_file_path}'
                GROUP BY {primary_key_column}
                HAVING COUNT(*) > 1
                LIMIT 1;
                """
                
                duplicates = conn.execute(check_sql).fetchall()
                
                if not duplicates:
                    self.logger.info(f"No duplicate primary keys found in {self.target_table}. Skipping fix.")
                    return
                
                self.logger.info(f"Found duplicate primary keys in {self.target_table}. Fixing...")
                
                # Step 2: Create a temp table that identifies all duplicate keys
                conn.execute(f"""
                CREATE TEMP TABLE duplicate_keys AS
                SELECT {primary_key_column}
                FROM '{transformed_file_path}'
                GROUP BY {primary_key_column}
                HAVING COUNT(*) > 1;
                """)
                
                # Step 3: Create temp tables for each part of the data we need to process
                # First, a table for all data with non-duplicate keys (we'll keep these as-is)
                conn.execute(f"""
                CREATE TEMP TABLE non_duplicate_data AS
                SELECT *
                FROM '{transformed_file_path}'
                WHERE {primary_key_column} NOT IN (SELECT {primary_key_column} FROM duplicate_keys);
                """)
                
                # Step 4: Create a table with numbered rows for duplicate key data
                conn.execute(f"""
                CREATE TEMP TABLE duplicate_data AS
                SELECT *, 
                    ROW_NUMBER() OVER (PARTITION BY {primary_key_column} ORDER BY (SELECT 1)) as dup_num
                FROM '{transformed_file_path}'
                WHERE {primary_key_column} IN (SELECT {primary_key_column} FROM duplicate_keys);
                """)
                
                # Step 5: Create a new table with fixed keys for duplicates
                conn.execute(f"""
                CREATE TEMP TABLE fixed_duplicate_data AS
                SELECT 
                    CASE 
                        WHEN dup_num = 1 THEN {primary_key_column}  -- Keep first occurrence unchanged
                        ELSE CAST(hash(CONCAT(CAST({primary_key_column} AS VARCHAR), CAST(dup_num AS VARCHAR))) % 9223372036854775807 AS {primary_key_type})
                    END AS {primary_key_column},
                    * EXCLUDE ({primary_key_column}, dup_num)
                FROM duplicate_data;
                """)
                
                # Step 6: Combine the fixed data back together and write to the original file
                conn.execute(f"""
                COPY (
                    SELECT * FROM non_duplicate_data
                    UNION ALL
                    SELECT * FROM fixed_duplicate_data
                ) TO '{transformed_file_path}' {constants.DUCKDB_FORMAT_STRING}
                """)
                
                self.logger.info(f"Successfully fixed duplicate primary keys in {self.target_table}")
        except Exception as e:
            raise Exception(f"Unable to handle duplicate primary keys in {self.target_table}: {str(e)}") from e
        finally:
            utils.close_duckdb_connection(conn, local_db_file)

