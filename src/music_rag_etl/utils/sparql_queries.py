def get_artists_by_year_range_query(
    start_year: int, end_year: int, limit: int, offset: int
) -> str:
    """
    Generate a SPARQL query to fetch artists active within a specific year range.

    Args:
        start_year: The first year of the period (inclusive).
        end_year: The last year of the period (inclusive).
        limit: The maximum number of results to return.
        offset: The offset from which to start fetching results.

    Returns:
        A formatted SPARQL query string.
    """
    # This query template is formatted with all necessary parameters.
    # Note the use of f-string interpolation for all dynamic values.
    return f"""
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX hint: <http://www.bigdata.com/queryHints#>
PREFIX schema: <http://schema.org/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?artist ?artistLabel ?date ?linkcount ?wikipedia_url
       (GROUP_CONCAT(DISTINCT ?genre_id; separator="|") AS ?genres)
       (GROUP_CONCAT(DISTINCT ?aliasLabel; separator="|") AS ?aliases)
       (SAMPLE(?label_en) AS ?artistLabel_en)
       (SAMPLE(?label_es) AS ?artistLabel_es)
       (SAMPLE(?label_fr) AS ?artistLabel_fr)
       (SAMPLE(?label_de) AS ?artistLabel_de)
WHERE {{
  hint:Query hint:optimizer "None" .

  VALUES ?root_genre {{
    wd:Q11399    # Rock music
    wd:Q9778     # Electronic music
    wd:Q37073    # Pop music
    wd:Q11366    # Alternative rock
    wd:Q187760   # New wave
    wd:Q1298934  # Synth-pop
    wd:Q598929   # Post-punk
    wd:Q178526   # Gothic rock
    wd:Q846083   # Dark wave
    wd:Q193606   # Trip hop
    wd:Q163891   # Experimental music
    wd:Q272167   # Shoegaze
    wd:Q596877   # Electronic body music (EBM)
    wd:Q170068   # Industrial music
    wd:Q38848    # Heavy metal music
    wd:Q786638   # Psychedelic music
    wd:Q76058    # Glam rock
    wd:Q58339    # Disco
  }}

  ?genre wdt:P279* ?root_genre .
  ?artist wdt:P136 ?genre .
  BIND(STRAFTER(STR(?genre), STR(wd:)) AS ?genre_id)
  ?artist wikibase:sitelinks ?linkcount .
  FILTER(?linkcount > 7)
  ?artist wdt:P31 ?type .
  FILTER(?type IN (wd:Q215380, wd:Q5))

  OPTIONAL {{ ?artist wdt:P571 ?inception . }}
  OPTIONAL {{ ?artist wdt:P2031 ?work_start . }}
  BIND(COALESCE(?inception, ?work_start) AS ?date)
  FILTER(?date >= "{start_year}-01-01"^^xsd:dateTime && ?date <= "{end_year}-12-31"^^xsd:dateTime)

  FILTER(
    ?type = wd:Q215380 ||
    EXISTS {{
      ?artist wdt:P106 ?occ .
      FILTER(?occ IN (
        wd:Q639669,  # Musician
        wd:Q177220,  # Singer
        wd:Q130857,  # Disc jockey (DJ)
        wd:Q486748,  # Composer
        wd:Q183945   # Record producer
      ))
    }}
  )

  OPTIONAL {{
    ?wikipedia_url schema:about ?artist ;
                   schema:isPartOf <https://en.wikipedia.org/> .
  }}
  OPTIONAL {{ ?artist skos:altLabel ?aliasLabel . FILTER (lang(?aliasLabel) = "en") }}
  OPTIONAL {{ ?artist rdfs:label ?label_en . FILTER (lang(?label_en) = "en") }}
  OPTIONAL {{ ?artist rdfs:label ?label_es . FILTER (lang(?label_es) = "es") }}
  OPTIONAL {{ ?artist rdfs:label ?label_fr . FILTER (lang(?label_fr) = "fr") }}
  OPTIONAL {{ ?artist rdfs:label ?label_de . FILTER (lang(?label_de) = "de") }}

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,es,en-gb,fr,de,[AUTO_LANGUAGE]". }}
}}
GROUP BY ?artist ?artistLabel ?date ?linkcount ?wikipedia_url

ORDER BY DESC(?linkcount)
LIMIT {limit}
OFFSET {offset}
"""
