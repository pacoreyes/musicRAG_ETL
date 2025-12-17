# musicRAG (ETL)

*Last update: December 17, 2025)*

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

#### Nodes (entities)

| Node   | Label  | Properties                                 |
|--------|--------|--------------------------------------------|
| Artist | Artist | - QID: string (Wikidata QID, Primary Key)
|        |        | - name: string
|        |        | - aliases: list of strings
|        |        | - country: string
|        |        | - tags: list of strings
|        |        | - similar_artists: list of strings
| Album  | Album  | - QID: string (Wikidata QID, Primary Key)
|        |        | - title: string
|        |        | - year: integer
| Track  | Track  | - QID: string (Wikidata QID, Primary Key)
|        |        | - title: string
| Genre  | Genre  | - QID: string (Wikidata QID, Primary Key)
|        |        | - name: string
|        |        | - aliases: list of strings

**Number of Articles**
- 14,003 articles initially collected.
- 13,810 articles (after deduplication by wikipedia_url, wikidata_id, artist name)
- 13,809 articles (finally processed, skipping 1 with empty wikipedia_url)

#### Edges (relationships)

| Edges                                 | Description                                    |
|---------------------------------------|------------------------------------------------|
| (Artist) - [:HAS_GENRE] -> (Genre)    | Connects an artist to their musical genres.    |
| (Album) - [:PERFORMED_BY] -> (Artist) | Connects an album to its performing artist(s). |
| (Album) - [:HAS_GENRE] -> (Genre)     | Connects an album to its genres.               |
| (Album) - [CONTAINS_TRACK] -> (Track  | Connects an album to its track.                |
| (Artist) - [SIMILAR_TO] -> (Artist)   | Connects an artist to other similar artists.   |

**Number of Nodes**
- Artist: 12,545
- Genre: 995
- Album: 120,436
- Track: 37,000
- *Total*: 170,977

**Number of Edges**
(upcoming)

## Tech Stack

The selection of the tech stack is based on two premises:
- Serverless architecture for stateful components.
- Scale to zero, avoid fixed costs.

(this document will be updated frequently)



