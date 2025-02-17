import logging
import time
from datetime import timedelta
from http import HTTPStatus

import httpx

logger = logging.getLogger(__name__)

def http_get(
    client: httpx.Client,
    url: str,
    timeout_s: int = 30,
    wait_default_delay: timedelta = timedelta(seconds=30),
    try_number: int = 1,
    max_tries: int = 5,
) -> httpx.Response:
    wait_period = wait_default_delay
    response = client.get(url=url, timeout=timeout_s)
    if response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
        retry_after_raw = response.headers.get("Retry-After")
        if retry_after_raw:
            logger.debug("Server sent 429 status code with Retry-After header value: %s", retry_after_raw)
            try:
                retry_after = timedelta(seconds=int(retry_after_raw))
                # sometimes server sends too low value in Retry-After header
                # so let's make sure we wait at least 30 seconds
                wait_period = max(retry_after, wait_default_delay)
            except ValueError:
                pass  # if Retry-After is not a number, we will use default wait_period
        logger.warning("Server sent 429 (Too Many Requests) status code. Going to sleep for: %s", wait_period)
        time.sleep(wait_period.total_seconds())
        return http_get(
            client=client,
            url=url,
            timeout_s=timeout_s,
            wait_default_delay=wait_period,
            try_number=try_number + 1,
            max_tries=max_tries,
        )
    response.raise_for_status()
    return response
