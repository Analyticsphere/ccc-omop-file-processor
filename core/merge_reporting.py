import core.constants as constants
import core.helpers.report_artifact as report_artifact
import core.reporting as reporting
import core.utils as utils
from core.storage_backend import storage

# Artifact names
SOURCE_SITE_ARTIFACT = "Merge source site"
SOURCE_DELIVERY_ROW_COUNT_ARTIFACT = "Merge source delivery row count"
TOTAL_ROW_COUNT_ARTIFACT = "Merge total row count"


class MergeReporter:
    """Generate provenance report artifacts describing a merge run's inputs."""

    @staticmethod
    def delivery_chunk_glob(merge_bucket: str, run_date: str, site: str, delivery_date: str) -> str:
        """
        Glob matching every table's chunk for one source (site, delivery_date).

        Chunk files are named <table>__<site>__<delivery_date>.parquet under
        merge_chunks/<table>/, so the leading */ matches each per-table subfolder.
        """
        return (
            f"{merge_bucket}/{run_date}/{constants.ArtifactPaths.MERGE_CHUNKS.value}"
            f"*/*__{site}__{delivery_date}{constants.PARQUET}"
        )

    @staticmethod
    def generate_delivery_row_count_sql(chunk_glob: str) -> str:
        """Count every row a single source delivery contributes to the merge."""
        glob = storage.get_uri(chunk_glob)
        return f"""
        SELECT COUNT(*) AS row_count
        FROM read_parquet('{glob}', union_by_name=true)
        """

    @staticmethod
    def generate_merge_report(merge_bucket: str, run_date: str, site: str, deliveries: list[dict]) -> None:
        """
        Create merge provenance artifacts, then consolidate them into the report CSV.

        Args:
            merge_bucket: The merged instance's bucket.
            run_date: The merged instance's delivery_date slot (US/Eastern run date).
            site: The synthetic merge site_name (used for the report CSV file name).
            deliveries: The source deliveries in this merge run, each {"site", "delivery_date"}.
        """
        MergeReporter._create_source_site_artifacts(merge_bucket, run_date, deliveries)
        total_rows = MergeReporter._create_source_delivery_row_count_artifacts(merge_bucket, run_date, deliveries)
        MergeReporter._create_total_row_count_artifact(merge_bucket, run_date, total_rows)
        MergeReporter._consolidate(merge_bucket, run_date, site)

    @staticmethod
    def _create_source_site_artifacts(merge_bucket: str, run_date: str, deliveries: list[dict]) -> None:
        """One artifact per distinct source site included in the merge."""
        sites = sorted({delivery["site"] for delivery in deliveries})
        utils.logger.info(f"Creating {len(sites)} merge source site artifact(s)")

        for site in sites:
            report_artifact.ReportArtifact(
                delivery_date=run_date,
                artifact_bucket=merge_bucket,
                concept_id=0,
                name=SOURCE_SITE_ARTIFACT,
                value_as_string=site,
                value_as_concept_id=0,
                value_as_number=None,
            ).save_artifact()

    @staticmethod
    def _create_source_delivery_row_count_artifacts(merge_bucket: str, run_date: str, deliveries: list[dict]) -> int:
        """
        One artifact per source delivery, carrying the rows it contributed to the merge.

        Returns the total rows across all deliveries (the sum of the per-delivery
        counts), so the caller can emit the merge-wide total without a second pass.
        """
        utils.logger.info(f"Creating {len(deliveries)} merge source delivery row-count artifact(s)")

        total_rows = 0
        for delivery in sorted(deliveries, key=lambda d: (d["site"], d["delivery_date"])):
            site = delivery["site"]
            delivery_date = delivery["delivery_date"]

            chunk_glob = MergeReporter.delivery_chunk_glob(merge_bucket, run_date, site, delivery_date)
            sql = MergeReporter.generate_delivery_row_count_sql(chunk_glob)
            result = utils.execute_duckdb_sql(
                sql,
                f"Unable to count merge input rows for {site} {delivery_date}",
                return_results=True,
            )
            row_count = result[0][0] if result else 0
            total_rows += row_count

            report_artifact.ReportArtifact(
                delivery_date=run_date,
                artifact_bucket=merge_bucket,
                concept_id=0,
                name=SOURCE_DELIVERY_ROW_COUNT_ARTIFACT,
                value_as_string=f"{site}__{delivery_date}",
                value_as_concept_id=0,
                value_as_number=float(row_count),
            ).save_artifact()

        return total_rows

    @staticmethod
    def _create_total_row_count_artifact(merge_bucket: str, run_date: str, total_rows: int) -> None:
        """One summary artifact: total rows across every delivery going into the merge."""
        utils.logger.info(f"Creating merge total row-count artifact: {total_rows} rows")

        report_artifact.ReportArtifact(
            delivery_date=run_date,
            artifact_bucket=merge_bucket,
            concept_id=0,
            name=TOTAL_ROW_COUNT_ARTIFACT,
            value_as_string=None,
            value_as_concept_id=0,
            value_as_number=float(total_rows),
        ).save_artifact()

    @staticmethod
    def _consolidate(merge_bucket: str, run_date: str, site: str) -> None:
        """
        Union the merge report parts into one CSV.

        Mirrors ReportGenerator._consolidate_report_files (reusing its consolidation
        SQL) but omits the per-site Connect participant summary, which is not
        meaningful for a merged instance.
        """
        report_tmp_dir = f"{run_date}/{constants.ArtifactPaths.REPORT_TMP.value}"
        tmp_files = utils.list_files(merge_bucket, report_tmp_dir, constants.PARQUET)

        if len(tmp_files) == 0:
            utils.logger.warning("No merge report artifacts found to consolidate")
            return

        file_paths = [
            storage.get_uri(f"{merge_bucket}/{report_tmp_dir}{file}")
            for file in tmp_files
        ]
        select_statement = " UNION ALL ".join(
            f"SELECT * FROM read_parquet('{path}')"
            for path in file_paths
        )

        output_path = storage.get_uri(
            f"{merge_bucket}/{run_date}/{constants.ArtifactPaths.REPORT.value}"
            f"delivery_report_{site}_{run_date}{constants.CSV}"
        )
        sql = reporting.ReportGenerator.generate_report_consolidation_sql(select_statement, output_path)
        utils.execute_duckdb_sql(sql, "Unable to consolidate merge report artifacts")
