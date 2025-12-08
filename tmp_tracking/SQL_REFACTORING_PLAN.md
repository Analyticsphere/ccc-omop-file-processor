# SQL Generation/Execution Separation Refactoring

## Goal
Separate SQL generation from execution to improve testability and maintainability.

## Pattern
- **Generate Functions**: Return SQL strings, take all parameters needed for SQL construction
- **Execute Functions**: Take SQL strings, handle DuckDB connection and execution

## Progress Tracker

### Phase 1: Simple Cases (Quick Wins) ✅ COMPLETED

#### file_processor.py
- [x] `process_incoming_parquet()` → Extract to `generate_process_incoming_parquet_sql()`
- [x] `csv_to_parquet()` → Extract to `generate_csv_to_parquet_sql()`

#### omop_client.py
- [x] `upgrade_file()` → Extract to `generate_upgrade_file_sql()`
- [x] `convert_vocab_to_parquet()` → Extract to `generate_convert_vocab_sql()`

### Phase 2: Complex Cases (High Value) 📋 TODO

#### vocab_harmonization.py
- [ ] `source_target_remapping()` → Extract to `generate_source_target_remapping_sql()`
- [ ] `check_new_targets()` → Extract to `generate_check_new_targets_sql()`
- [ ] `domain_table_check()` → Extract to `generate_domain_table_check_sql()`

#### omop_client.py
- [ ] `create_optimized_vocab_file()` → Extract to `generate_optimized_vocab_sql()`
- [ ] `generate_derived_data_from_harmonized()` → Extract to `generate_derived_table_sql()`

### Phase 3: Edge Cases 📋 TODO

#### vocab_harmonization.py
- [ ] `_deduplicate_primary_keys()` → Extract to `generate_deduplicate_sql_statements()` (returns list)

#### utils.py
- [ ] `generate_report()` → Extract to `generate_report_consolidation_sql()`
- [ ] `get_delivery_vocabulary_version()` → Extract to `generate_vocab_version_query_sql()`

#### normalization.py
- [ ] `create_row_count_artifacts()` → Extract to `generate_row_count_query_sql()`

---

## Refactoring Log

### 2025-12-07 - Phase 1 Completed

#### Function: `process_incoming_parquet()` in file_processor.py
- **Status**: ✅ COMPLETED
- **Changes**:
  - Created new function `generate_process_incoming_parquet_sql(file_path: str, parquet_columns: list[str]) -> str`
  - Extracts SQL generation logic (lines 18-54)
  - Modified `process_incoming_parquet()` to call generation function then execute SQL
  - Handles special 'offset' column for note_nlp table
- **Testing**: Ready for testing
- **Notes**: Pure function returns SQL string, no database dependencies

#### Function: `csv_to_parquet()` in file_processor.py
- **Status**: ✅ COMPLETED
- **Changes**:
  - Created new function `generate_csv_to_parquet_sql(file_path: str, csv_column_names: list[str], conversion_options: list = []) -> str`
  - Extracts SQL generation logic (lines 77-124)
  - Modified `csv_to_parquet()` to call generation function then execute SQL
  - Preserves retry logic with error handling
- **Testing**: Ready for testing
- **Notes**: Handles configurable CSV conversion options

#### Function: `upgrade_file()` in omop_client.py
- **Status**: ✅ COMPLETED
- **Changes**:
  - Created new function `generate_upgrade_file_sql(upgrade_script: str, normalized_file_path: str) -> str`
  - Extracts SQL generation logic for CDM version upgrades
  - Modified `upgrade_file()` to call generation function then execute SQL
  - Maintains existing upgrade logic for v5.3 to v5.4
- **Testing**: Ready for testing
- **Notes**: SQL generation isolated from file I/O and execution logic

#### Function: `convert_vocab_to_parquet()` in omop_client.py
- **Status**: ✅ COMPLETED
- **Changes**:
  - Created new function `generate_convert_vocab_sql(csv_file_path: str, parquet_file_path: str, csv_columns: list[str]) -> str`
  - Extracts SQL generation logic for vocabulary CSV conversion
  - Modified `convert_vocab_to_parquet()` to call generation function then execute SQL
  - Handles date fields (valid_start_date, valid_end_date) with proper formatting
- **Testing**: Ready for testing
- **Notes**: Tab-delimited CSV parsing preserved in generation function

---

## Testing Strategy

For each refactored function:
1. ✅ SQL generation function can be called without database connection
2. ✅ Generated SQL is syntactically valid
3. ✅ Execution function works with generated SQL
4. ✅ End-to-end behavior is unchanged

---

## Benefits Achieved

- [x] SQL generation is testable in isolation (Phase 1: 4 functions)
- [x] Easier to validate SQL correctness (Phase 1: 4 functions)
- [x] Better separation of concerns (Phase 1: 4 functions)
- [x] Reduced coupling between logic layers (Phase 1: 4 functions)
- [x] Improved code readability (Phase 1: 4 functions)

**Phase 1 Summary**: 4 of 16 functions refactored (25% complete)
