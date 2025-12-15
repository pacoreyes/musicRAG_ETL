import polars as pl


def clean_text(df: pl.DataFrame, col_name: str) -> pl.DataFrame:
    return df.with_columns(
        pl.col(col_name)
        # 1. Manual replacements (escaped quotes & newlines)
        .str.replace_all(r'\\"', '"')  # Fix escaped quotes
        .str.replace_all(r"[\n\r]+", " ")  # Replace newlines with space

        # 2. Unicode Normalization (Optional: handles partial ftfy work)
        #.str.normalize("NFKC")

        # 3. Cleantext equivalent (extra_spaces=True)
        .str.replace_all(r"\s+", " ")  # Squash multiple spaces
        .str.strip_chars()  # Remove leading/trailing space
    )
