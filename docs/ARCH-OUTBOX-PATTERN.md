# Architectural Design: Transactional Outbox Pattern

## Current Implementation: Level 2 (Dedicated Relay Worker)
We have implemented the **Transactional Outbox Pattern** to solve the "Dual Write" problem between PostgreSQL (Metadata) and Qdrant (Vector Search).

### Why we use this:
1. **Consistency:** A product is only saved if the indexing task is also saved. We avoid "Ghost Products" that exist in the DB but aren't searchable.
2. **Reliability:** If Qdrant is down, the tasks stay safely in the Postgres `indexing_outbox` table.
3. **Isolation:** The Relay runs as a separate process. If it crashes, it doesn't affect the main data ingestion API.

### Scaling to "Production Grade" (Level 3)
In the future, if the system reaches millions of products per day, we may transition to **Change Data Capture (CDC)** using **Debezium and Kafka**.

#### Why we didn't use Debezium/Kafka yet:
- **Complexity Overhead:** CDC requires managing Zookeeper, Kafka Brokers, and Debezium Connectors. For our current scale, this is "over-engineering."
- **Maintenance:** Level 2 allows us to use our existing PostgreSQL knowledge without adding two new complex systems to the stack.
- **Polling is sufficient:** Our current throughput can easily be handled by standard SQL polling every few seconds.

#### When to switch to Level 3:
- If the database load from "polling" the outbox table exceeds 10-15% of total CPU.
- If we need sub-second search synchronization across many different microservices.

### Implementation Details:
- **Table:** `indexing_outbox` (Postgres)
- **Producer:** `src/repository.py:ingest_product_atomic`
- **Consumer:** `src/pipeline/relay.py` (Polling every 5s)
- **Deployment:** Separate container `indexer-relay` in `docker-compose.yml`.
