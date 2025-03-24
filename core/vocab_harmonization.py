import core.constants as constants
import core.utils as utils

def harominze_parquet_file(table_name: str, cdm_version: str, site: str, site_bucket: str, delivery_date: str, vocab_version: str, vocab_gcs_bucket: str) -> None:
    get_source_target_mapping_sql(
        table_name,
        cdm_version,
        site,
        site_bucket,
        delivery_date,
        vocab_version,
        vocab_gcs_bucket
    )

def get_vocab_harmization_sql(type: str, table_name: str) -> str:
    # Works only on 'normalized' v5.4
    if type == constants.SOURCE_TARGET:
        sql = get_source_target_mapping_sql(table_name)


def get_source_target_mapping_sql(table_name: str, cdm_version: str, site: str, site_bucket: str, delivery_date: str, vocab_version: str, vocab_gcs_bucket: str) -> str:
    utils.logger.warning(f"IN get_source_target_mapping_sql()")

    schema = utils.get_table_schema(table_name, cdm_version)

    columns = schema[table_name]["columns"]
    ordered_omop_columns = list(columns.keys())  # preserve column order

    # Get _concept_id and _source_concept_id columns for table
    target_concept_id_column = constants.SOURCE_TARGET_COLUMNS[table_name]['target_concept_id']
    source_concept_id_column = constants.SOURCE_TARGET_COLUMNS[table_name]['source_concept_id']
    primary_key = utils.get_primary_key_field(table_name, cdm_version)
    utils.logger.warning(f"target_concept_id_column is {target_concept_id_column} and source_concept_id_column is {source_concept_id_column} and primary_key is {primary_key}")

    # specimen and note tables don't have _source_concept_id columns so can't be evaulated with this method
    if not source_concept_id_column or source_concept_id_column == "":
        return ""

    initial_select_exprs: list = []
    final_select_exprs: list = []

    for column_name in ordered_omop_columns:
        column_name = f"tbl.{column_name}"
        final_select_exprs.append(column_name)

        # Replace new target concept_id in target_concept_id_column
        if column_name == target_concept_id_column:
            column_name = f"vocab.target_concept_id AS {target_concept_id_column}"
            #initial_select_exprs.append(column_name)

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
        FROM @{table_name.upper()} AS tbl
        INNER JOIN @OPTIMIZED_VOCABULARY AS vocab
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
        FROM @{table_name.upper()} AS tbl
        INNER JOIN @OPTIMIZED_VOCABULARY AS vocab 
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
        ELSE '{table_name}' END AS target_table
    """
    final_select_exprs.append(case_when_target_table)
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

    final_sql = utils.placeholder_to_table_path(site, site_bucket, delivery_date, final_cte, vocab_version, vocab_gcs_bucket)

    final_sql_no_return = final_sql.replace('\n',' ')
    utils.logger.warning(f"*/*/*/ final_cte is {final_sql_no_return}")

