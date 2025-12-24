from typing import Callable

from dagster import asset, AssetExecutionContext

from music_rag_etl.utils.sparql_queries import get_artists_by_year_range_query
from music_rag_etl.utils.io_helpers import merge_jsonl_files
from music_rag_etl.utils.wikidata_helpers import (
    execute_sparql_extraction,
    format_artist_record_from_sparql,
)
from music_rag_etl.settings import (
    PATH_DATASETS,
    DECADES_TO_EXTRACT,
    ARTIST_INDEX_PRE_CLEAN,
)


def create_artist_extraction_asset(decade: str, year_range: tuple[int, int]) -> Callable:
    """
    Asset factory for creating a Dagster asset that extracts artist data for a
    specific decade from Wikidata.

    Args:
        decade: The label for the decade (e.g., "80s").
        year_range: A tuple containing the start and end year of the decade.

    Returns:
        A Dagster asset function.
    """
    start_year, end_year = year_range
    output_filename = f"artist_index_{decade}.jsonl"
    output_path = PATH_DATASETS / output_filename

    @asset(
        name=f"build_artist_index_{decade}",
        description="Factory function",
        group_name="extraction"
    )
    def _extraction_asset(context: AssetExecutionContext) -> str:
        """
        Extracts artist data for a specific decade and saves it to a JSONL file.
        """
        execute_sparql_extraction(
            context=context,
            output_path=output_path,
            get_query_function=get_artists_by_year_range_query,
            record_processor=format_artist_record_from_sparql,
            label=f"artists_{decade}",
            start_year=start_year,
            end_year=end_year,
        )

        context.log.info(f"Finished extraction for {decade}. Output at {output_path}")
        return str(output_path)

    return _extraction_asset


def build_artist_extraction_assets():
    """
    Generates and returns a list of all artist extraction assets by dynamically
    creating them from the decades defined in settings.
    """
    assets = []
    for decade, year_range in DECADES_TO_EXTRACT.items():
        extraction_asset = create_artist_extraction_asset(decade, year_range)
        assets.append(extraction_asset)
    return assets


@asset(
    name="build_artist_index",
    deps=[f"build_artist_index_{decade}" for decade in DECADES_TO_EXTRACT],
    description="Merges all decade-specific artist JSONL files into a single artist_index.jsonl.",
    group_name="extraction"
)
def build_artist_index(context: AssetExecutionContext) -> str:
    """
    Merges all decade-specific artist JSONL files into a single artist_index.jsonl.
    """
    output_path = PATH_DATASETS / ARTIST_INDEX_PRE_CLEAN
    input_paths = [
        PATH_DATASETS / f"artist_index_{decade}.jsonl" for decade in DECADES_TO_EXTRACT
    ]
    merge_jsonl_files(input_paths, output_path)
    context.log.info(f"Merged artist index saved to {output_path}")
    return str(output_path)


# Dynamically create all artist extraction assets
# and expose them for Dagster to discover.
artist_extraction_assets = build_artist_extraction_assets()
