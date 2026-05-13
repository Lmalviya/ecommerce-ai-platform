import logging
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)
import httpx

logger = logging.getLogger(__name__)

# Policy for AI/External APIs (longer backoff, more attempts)
retry_ai_api = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=15),
    retry=retry_if_exception_type((httpx.HTTPError, ConnectionError, TimeoutError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True
)

# Policy for Internal Services like Qdrant or MinIO (shorter backoff)
retry_internal_service = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=1, max=5),
    retry=retry_if_exception_type((httpx.HTTPError, ConnectionError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True
)
