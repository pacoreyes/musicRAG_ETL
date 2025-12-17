# musicRAG (ETL)

This is an ETL pipeline made in [Dagster](https://dagster.io/), which orchestrates the data ingestion from multiple sources: 

- Wikidata API using SPARQL
- Wikipedia API, and 
- Last FM API

The goal of this ETL system is to prepare the unstructured data of Wikipedia articles of musicians, bands, and other musical artists in a dataset split in chunks enriched with structured data (metadata) ingested from multiple sources. 
We make an extensive use of [Polars](https://pola.rs/) due to its velocity to manipulate data in the transformation stage.
We also use extensively [dlt (Data Load Tool)](https://dlthub.com/) to move data from one point to another in a scenery where data schema is permanently evolving, making automatic the always challenging schema evolution. And for handling data validation, we use [Pydantic](https://pydantic.dev/).
In this project we prepare data for two different data search approaches:

- Semantic: using a vector database, [Chroma](https://www.trychroma.com/).
- Deterministic: using a graph dtabase, [Memgraph](https://memgraph.com/).

The semantic search with Chroma is mostly probabilistic, although we have certain control of filtering and orchestration leveraging the metadata. On the other hand, the relational search with Memgraph is deterministic, because it relies on a rigid data structure among entities (nodes) and relations (edges).
This double sword solution relies on a well thought data engineering work.

## Data

### 1. Vector Database

Each document in Chroma is a chunk or a larger Wikipedia Article. It consists of the text content itself (which is vectorized) and a set of metadata tags.

| Field    | Type         | Description                                        |
|----------|--------------|----------------------------------------------------|
| article  | String       | The unstructured text that is vectorized to enable |
|          |              | enable semantic similarity search.                 |
| metadata | JSON object  | - title (string)
|          |              | - artist_name (string)
|          |              | - genres (list of strings)
|          |              | - inception_year (integer)
|          |              | - wikipedia_url (string)
|          |              | - wikidata_entity (string)
|          |              | - relevance_score (float)
|          |              | - chunk_index (integer)
|          |              | - total_chunks (integer)

### 2. Graph Database

Each node in Memgraph is an entity (musician, band, musical artist) with properties. And each edge is a relationship between entities.

#### Nodes

| Node   | Label  | Properties                                 |
| Artist | Artist | - id: string (Wikidata QID, Primary Key)
|        |        | - name: string
|        |        | - aliases: list of strings
|        |        | - country: string
|        |        | - tags: list of strings
|        |        | - similar_artists: list of strings
| Album  | Album  | - id: string (Wikidata QID, Primary Key)
|        |        | - title: string
|        |        | - year: integer
| Track  | Track  |



id: string (Wikidata QID, Primary Key)
title: string
year: integer
Track
Label: :Track
Properties:
id: string (Wikidata QID, Primary Key)
title: string
Genre
Label: :Genre
Properties:
id: string (Wikidata QID, Primary Key)
name: string
aliases: list of strings





Also, the selection of the 


## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `music_rag_etl/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `music_rag_etl_tests` directory and you can run tests using `pytest`:

```bash
pytest music_rag_etl_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/guides/automate/schedules/) or [Sensors](https://docs.dagster.io/guides/automate/sensors/) for your jobs, the [Dagster Daemon](https://docs.dagster.io/guides/deploy/execution/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deploy on Dagster+

The easiest way to deploy your Dagster project is to use Dagster+.

Check out the [Dagster+ documentation](https://docs.dagster.io/dagster-plus/) to learn more.
