import logging
from typing import Any, Dict
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def extract_database_credentials(database_url) -> Dict[str, Any]:
    """Extract database credentials from the database URL.
    :return: database credentials
    :rtype: Dict[str, Any]
    """
    parsed_url = urlparse(database_url)
    return {
        "host": parsed_url.hostname,
        "port": parsed_url.port or 3306,
        "user": parsed_url.username,
        "password": parsed_url.password,
        "db": parsed_url.path[1:],
    }


async def close_db_pool(dp_pool):
    logger.debug('closing db pool %r', dp_pool)
    dp_pool.close()
    await dp_pool.wait_closed()
