import logging
from src.database import DatabaseClient

logger = logging.getLogger(__name__)

def run_migrations():
    """
    Ensures the required tables for the Outbox pattern exist in the target database.
    """
    client = DatabaseClient.get_instance()
    
    sql = """
    CREATE TABLE IF NOT EXISTS indexing_outbox (
        id SERIAL PRIMARY KEY,
        product_id VARCHAR(255) NOT NULL,
        payload JSONB NOT NULL,
        status VARCHAR(20) DEFAULT 'PENDING',
        retry_count INT DEFAULT 0,
        last_error TEXT,
        next_retry_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        processed_at TIMESTAMP WITH TIME ZONE
    );

    CREATE INDEX IF NOT EXISTS idx_outbox_status_retry 
    ON indexing_outbox(status, next_retry_at) 
    WHERE status IN ('PENDING', 'FAILED');
    """
    
    try:
        with client.pool.connection() as conn:
            conn.execute(sql)
            conn.commit()
        logger.info("Migrations completed successfully: indexing_outbox table is ready.")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_migrations()
