from typing import List

from dagster import asset

from music_rag_etl.utils.extraction_helpers import run_extraction, process_artist_record
from music_rag_etl.utils.sparql_queries import get_artists_by_year_range_query

from music_rag_etl.settings import PATH_TEMP


@asset
def create_artist_indexes():
    """
    This asset orchestrates the extraction of artist data from Wikidata for
    multiple decades and saves it into JSONL files.
    """

    def create_artist_index(
        start_year: int, end_year: int, output_filename: str, label: str
    ) -> str:
        """
        Fetches artists for a specific year range and saves them to a file.
        """
        output_path = PATH_TEMP / output_filename

        print(
            f"Starting artist extraction for {start_year}-{end_year} "
            f"to {output_path}"
        )

        run_extraction(
            output_path=output_path,
            get_query_function=get_artists_by_year_range_query,
            record_processor=process_artist_record,
            start_year=start_year,
            end_year=end_year,
            label=label,
        )

        return str(output_path)

    def run_all_decades() -> List[str]:
        """
        Runs the artist extraction process for all decades and returns a list of the
        generated output file paths.
        """
        output_filenames = []
        decades = {
            "60s": (1960, 1969),
            "70s": (1970, 1979),
            "80s": (1980, 1989),
            "90s": (1990, 1999),
            "00s": (2000, 2009),
            "10s": (2010, 2019),
            "20s": (2020, 2029),
        }

        for label, (start_year, end_year) in decades.items():
            filename_only = f"artist_index_{label}.jsonl"
            full_path = str(PATH_TEMP / filename_only)
            output_filenames.append(full_path)

            print(f"\n--- Running extraction for {label} ({start_year}-{end_year}) ---")
            create_artist_index(
                start_year=start_year,
                end_year=end_year,
                output_filename=filename_only,
                label=f"artists_{label}",
            )
            print(f"--- Finished extraction for {label} ---")

        return output_filenames

    generated_files = run_all_decades()
    return generated_files