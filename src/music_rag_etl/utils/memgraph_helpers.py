from dagster import Config, AssetExecutionContext
from pydantic import Field
from gqlalchemy import Memgraph


class MemgraphConfig(Config):
    """Configuration for Memgraph connection."""
    host: str = Field("127.0.0.1", description="Memgraph host address.")
    port: int = Field(7687, description="Memgraph port number.")


def get_memgraph_client(config: MemgraphConfig) -> Memgraph:
    """
    Initializes and returns a Memgraph client based on the provided configuration.

    Args:
        config: The Memgraph configuration object containing host and port.

    Returns:
        A Memgraph client instance connected to the specified host and port.
    """
    return Memgraph(host=config.host, port=config.port)


def clear_database(memgraph: Memgraph, context: AssetExecutionContext) -> None:
    """
    Clears all nodes, relationships, and indexes from the database.
    
    This function performs a complete cleanup of the Memgraph database instance,
    removing all data and dropping all indexes. It is typically used as a 
    preparatory step before a fresh data ingestion.

    Args:
        memgraph: The Memgraph client instance to execute queries.
        context: The Dagster asset execution context for logging.
    """
    context.log.info("Starting database cleanup...")

    # 1. Delete all nodes and relationships
    memgraph.execute("MATCH (n) DETACH DELETE n;")
    context.log.info("Deleted all nodes and relationships.")

    # 2. Drop all indexes
    # Fetch existing indexes
    indexes = list(memgraph.execute_and_fetch("SHOW INDEX INFO;"))
    for idx in indexes:
        label = idx.get("label")
        property_name = idx.get("property")
        if label and property_name:
            query = f"DROP INDEX ON :{label}({property_name});"
            try:
                memgraph.execute(query)
                context.log.info(f"Dropped index: :{label}({property_name})")
            except Exception as e:
                context.log.warning(
                    f"Failed to drop index :{label}({property_name}). Error: {e}"
                )